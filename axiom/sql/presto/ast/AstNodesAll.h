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

#include "axiom/sql/presto/ast/AstExpressions.h"
#include "axiom/sql/presto/ast/AstFunctions.h"
#include "axiom/sql/presto/ast/AstLiterals.h"
#include "axiom/sql/presto/ast/AstNode.h"
#include "axiom/sql/presto/ast/AstRelations.h"
#include "axiom/sql/presto/ast/AstStatements.h"
#include "axiom/sql/presto/ast/AstSupport.h"

namespace axiom::sql::presto {

using AstNodePtr = std::shared_ptr<Node>;
using AstExpressionPtr = std::shared_ptr<Expression>;
using AstStatementPtr = std::shared_ptr<Statement>;
using AstRelationPtr = std::shared_ptr<Relation>;

} // namespace axiom::sql::presto
