/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

// Generated from PrestoSql.g4 by ANTLR 4.9.3

#include "axiom/sql/presto/PrestoSqlListener.h"
#include "axiom/sql/presto/PrestoSqlVisitor.h"

#include "axiom/sql/presto/PrestoSqlParser.h"

using namespace antlrcpp;
using namespace facebook::velox::sql;
using namespace antlr4;

PrestoSqlParser::PrestoSqlParser(TokenStream* input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(
      this, _atn, _decisionToDFA, _sharedContextCache);
}

PrestoSqlParser::~PrestoSqlParser() {
  delete _interpreter;
}

std::string PrestoSqlParser::getGrammarFileName() const {
  return "PrestoSql.g4";
}

const std::vector<std::string>& PrestoSqlParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& PrestoSqlParser::getVocabulary() const {
  return _vocabulary;
}

//----------------- SingleStatementContext
//------------------------------------------------------------------

PrestoSqlParser::SingleStatementContext::SingleStatementContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::StatementContext*
PrestoSqlParser::SingleStatementContext::statement() {
  return getRuleContext<PrestoSqlParser::StatementContext>(0);
}

tree::TerminalNode* PrestoSqlParser::SingleStatementContext::EOF() {
  return getToken(PrestoSqlParser::EOF, 0);
}

size_t PrestoSqlParser::SingleStatementContext::getRuleIndex() const {
  return PrestoSqlParser::RuleSingleStatement;
}

void PrestoSqlParser::SingleStatementContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSingleStatement(this);
}

void PrestoSqlParser::SingleStatementContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSingleStatement(this);
}

antlrcpp::Any PrestoSqlParser::SingleStatementContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSingleStatement(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::SingleStatementContext* PrestoSqlParser::singleStatement() {
  SingleStatementContext* _localctx =
      _tracker.createInstance<SingleStatementContext>(_ctx, getState());
  enterRule(_localctx, 0, PrestoSqlParser::RuleSingleStatement);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(178);
    statement();
    setState(179);
    match(PrestoSqlParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StandaloneExpressionContext
//------------------------------------------------------------------

PrestoSqlParser::StandaloneExpressionContext::StandaloneExpressionContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::StandaloneExpressionContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::StandaloneExpressionContext::EOF() {
  return getToken(PrestoSqlParser::EOF, 0);
}

size_t PrestoSqlParser::StandaloneExpressionContext::getRuleIndex() const {
  return PrestoSqlParser::RuleStandaloneExpression;
}

void PrestoSqlParser::StandaloneExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterStandaloneExpression(this);
}

void PrestoSqlParser::StandaloneExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitStandaloneExpression(this);
}

antlrcpp::Any PrestoSqlParser::StandaloneExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitStandaloneExpression(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::StandaloneExpressionContext*
PrestoSqlParser::standaloneExpression() {
  StandaloneExpressionContext* _localctx =
      _tracker.createInstance<StandaloneExpressionContext>(_ctx, getState());
  enterRule(_localctx, 2, PrestoSqlParser::RuleStandaloneExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(181);
    expression();
    setState(182);
    match(PrestoSqlParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StandaloneRoutineBodyContext
//------------------------------------------------------------------

PrestoSqlParser::StandaloneRoutineBodyContext::StandaloneRoutineBodyContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::RoutineBodyContext*
PrestoSqlParser::StandaloneRoutineBodyContext::routineBody() {
  return getRuleContext<PrestoSqlParser::RoutineBodyContext>(0);
}

tree::TerminalNode* PrestoSqlParser::StandaloneRoutineBodyContext::EOF() {
  return getToken(PrestoSqlParser::EOF, 0);
}

size_t PrestoSqlParser::StandaloneRoutineBodyContext::getRuleIndex() const {
  return PrestoSqlParser::RuleStandaloneRoutineBody;
}

void PrestoSqlParser::StandaloneRoutineBodyContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterStandaloneRoutineBody(this);
}

void PrestoSqlParser::StandaloneRoutineBodyContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitStandaloneRoutineBody(this);
}

antlrcpp::Any PrestoSqlParser::StandaloneRoutineBodyContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitStandaloneRoutineBody(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::StandaloneRoutineBodyContext*
PrestoSqlParser::standaloneRoutineBody() {
  StandaloneRoutineBodyContext* _localctx =
      _tracker.createInstance<StandaloneRoutineBodyContext>(_ctx, getState());
  enterRule(_localctx, 4, PrestoSqlParser::RuleStandaloneRoutineBody);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(184);
    routineBody();
    setState(185);
    match(PrestoSqlParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StatementContext
//------------------------------------------------------------------

PrestoSqlParser::StatementContext::StatementContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::StatementContext::getRuleIndex() const {
  return PrestoSqlParser::RuleStatement;
}

void PrestoSqlParser::StatementContext::copyFrom(StatementContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ExplainContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ExplainContext::EXPLAIN() {
  return getToken(PrestoSqlParser::EXPLAIN, 0);
}

PrestoSqlParser::StatementContext*
PrestoSqlParser::ExplainContext::statement() {
  return getRuleContext<PrestoSqlParser::StatementContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ExplainContext::ANALYZE() {
  return getToken(PrestoSqlParser::ANALYZE, 0);
}

tree::TerminalNode* PrestoSqlParser::ExplainContext::VERBOSE() {
  return getToken(PrestoSqlParser::VERBOSE, 0);
}

std::vector<PrestoSqlParser::ExplainOptionContext*>
PrestoSqlParser::ExplainContext::explainOption() {
  return getRuleContexts<PrestoSqlParser::ExplainOptionContext>();
}

PrestoSqlParser::ExplainOptionContext*
PrestoSqlParser::ExplainContext::explainOption(size_t i) {
  return getRuleContext<PrestoSqlParser::ExplainOptionContext>(i);
}

PrestoSqlParser::ExplainContext::ExplainContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ExplainContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterExplain(this);
}
void PrestoSqlParser::ExplainContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitExplain(this);
}

antlrcpp::Any PrestoSqlParser::ExplainContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExplain(this);
  else
    return visitor->visitChildren(this);
}
//----------------- PrepareContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::PrepareContext::PREPARE() {
  return getToken(PrestoSqlParser::PREPARE, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::PrepareContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::PrepareContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

PrestoSqlParser::StatementContext*
PrestoSqlParser::PrepareContext::statement() {
  return getRuleContext<PrestoSqlParser::StatementContext>(0);
}

PrestoSqlParser::PrepareContext::PrepareContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::PrepareContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrepare(this);
}
void PrestoSqlParser::PrepareContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrepare(this);
}

antlrcpp::Any PrestoSqlParser::PrepareContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitPrepare(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DropMaterializedViewContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DropMaterializedViewContext::DROP() {
  return getToken(PrestoSqlParser::DROP, 0);
}

tree::TerminalNode*
PrestoSqlParser::DropMaterializedViewContext::MATERIALIZED() {
  return getToken(PrestoSqlParser::MATERIALIZED, 0);
}

tree::TerminalNode* PrestoSqlParser::DropMaterializedViewContext::VIEW() {
  return getToken(PrestoSqlParser::VIEW, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::DropMaterializedViewContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::DropMaterializedViewContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::DropMaterializedViewContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

PrestoSqlParser::DropMaterializedViewContext::DropMaterializedViewContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DropMaterializedViewContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDropMaterializedView(this);
}
void PrestoSqlParser::DropMaterializedViewContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDropMaterializedView(this);
}

antlrcpp::Any PrestoSqlParser::DropMaterializedViewContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDropMaterializedView(this);
  else
    return visitor->visitChildren(this);
}
//----------------- UseContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::UseContext::USE() {
  return getToken(PrestoSqlParser::USE, 0);
}

std::vector<PrestoSqlParser::IdentifierContext*>
PrestoSqlParser::UseContext::identifier() {
  return getRuleContexts<PrestoSqlParser::IdentifierContext>();
}

PrestoSqlParser::IdentifierContext* PrestoSqlParser::UseContext::identifier(
    size_t i) {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(i);
}

PrestoSqlParser::UseContext::UseContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::UseContext::enterRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterUse(this);
}
void PrestoSqlParser::UseContext::exitRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitUse(this);
}

antlrcpp::Any PrestoSqlParser::UseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUse(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AddConstraintContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::AddConstraintContext::ALTER() {
  return getToken(PrestoSqlParser::ALTER, 0);
}

tree::TerminalNode* PrestoSqlParser::AddConstraintContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::AddConstraintContext::ADD() {
  return getToken(PrestoSqlParser::ADD, 0);
}

PrestoSqlParser::ConstraintSpecificationContext*
PrestoSqlParser::AddConstraintContext::constraintSpecification() {
  return getRuleContext<PrestoSqlParser::ConstraintSpecificationContext>(0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::AddConstraintContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::AddConstraintContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::AddConstraintContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

PrestoSqlParser::AddConstraintContext::AddConstraintContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::AddConstraintContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAddConstraint(this);
}
void PrestoSqlParser::AddConstraintContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitAddConstraint(this);
}

antlrcpp::Any PrestoSqlParser::AddConstraintContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAddConstraint(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DeallocateContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DeallocateContext::DEALLOCATE() {
  return getToken(PrestoSqlParser::DEALLOCATE, 0);
}

tree::TerminalNode* PrestoSqlParser::DeallocateContext::PREPARE() {
  return getToken(PrestoSqlParser::PREPARE, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::DeallocateContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::DeallocateContext::DeallocateContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DeallocateContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDeallocate(this);
}
void PrestoSqlParser::DeallocateContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDeallocate(this);
}

antlrcpp::Any PrestoSqlParser::DeallocateContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDeallocate(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RenameTableContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RenameTableContext::ALTER() {
  return getToken(PrestoSqlParser::ALTER, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameTableContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameTableContext::RENAME() {
  return getToken(PrestoSqlParser::RENAME, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameTableContext::TO() {
  return getToken(PrestoSqlParser::TO, 0);
}

std::vector<PrestoSqlParser::QualifiedNameContext*>
PrestoSqlParser::RenameTableContext::qualifiedName() {
  return getRuleContexts<PrestoSqlParser::QualifiedNameContext>();
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::RenameTableContext::qualifiedName(size_t i) {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(i);
}

tree::TerminalNode* PrestoSqlParser::RenameTableContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameTableContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

PrestoSqlParser::RenameTableContext::RenameTableContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RenameTableContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRenameTable(this);
}
void PrestoSqlParser::RenameTableContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRenameTable(this);
}

antlrcpp::Any PrestoSqlParser::RenameTableContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRenameTable(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CommitContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CommitContext::COMMIT() {
  return getToken(PrestoSqlParser::COMMIT, 0);
}

tree::TerminalNode* PrestoSqlParser::CommitContext::WORK() {
  return getToken(PrestoSqlParser::WORK, 0);
}

PrestoSqlParser::CommitContext::CommitContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CommitContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCommit(this);
}
void PrestoSqlParser::CommitContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCommit(this);
}

antlrcpp::Any PrestoSqlParser::CommitContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCommit(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateRoleContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CreateRoleContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateRoleContext::ROLE() {
  return getToken(PrestoSqlParser::ROLE, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::CreateRoleContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateRoleContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateRoleContext::ADMIN() {
  return getToken(PrestoSqlParser::ADMIN, 0);
}

PrestoSqlParser::GrantorContext* PrestoSqlParser::CreateRoleContext::grantor() {
  return getRuleContext<PrestoSqlParser::GrantorContext>(0);
}

PrestoSqlParser::CreateRoleContext::CreateRoleContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CreateRoleContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCreateRole(this);
}
void PrestoSqlParser::CreateRoleContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCreateRole(this);
}

antlrcpp::Any PrestoSqlParser::CreateRoleContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCreateRole(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowCreateFunctionContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowCreateFunctionContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowCreateFunctionContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowCreateFunctionContext::FUNCTION() {
  return getToken(PrestoSqlParser::FUNCTION, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::ShowCreateFunctionContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::TypesContext*
PrestoSqlParser::ShowCreateFunctionContext::types() {
  return getRuleContext<PrestoSqlParser::TypesContext>(0);
}

PrestoSqlParser::ShowCreateFunctionContext::ShowCreateFunctionContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowCreateFunctionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowCreateFunction(this);
}
void PrestoSqlParser::ShowCreateFunctionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowCreateFunction(this);
}

antlrcpp::Any PrestoSqlParser::ShowCreateFunctionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowCreateFunction(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DropColumnContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DropColumnContext::ALTER() {
  return getToken(PrestoSqlParser::ALTER, 0);
}

tree::TerminalNode* PrestoSqlParser::DropColumnContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::DropColumnContext::DROP() {
  return getToken(PrestoSqlParser::DROP, 0);
}

tree::TerminalNode* PrestoSqlParser::DropColumnContext::COLUMN() {
  return getToken(PrestoSqlParser::COLUMN, 0);
}

std::vector<PrestoSqlParser::QualifiedNameContext*>
PrestoSqlParser::DropColumnContext::qualifiedName() {
  return getRuleContexts<PrestoSqlParser::QualifiedNameContext>();
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::DropColumnContext::qualifiedName(size_t i) {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(i);
}

std::vector<tree::TerminalNode*> PrestoSqlParser::DropColumnContext::IF() {
  return getTokens(PrestoSqlParser::IF);
}

tree::TerminalNode* PrestoSqlParser::DropColumnContext::IF(size_t i) {
  return getToken(PrestoSqlParser::IF, i);
}

std::vector<tree::TerminalNode*> PrestoSqlParser::DropColumnContext::EXISTS() {
  return getTokens(PrestoSqlParser::EXISTS);
}

tree::TerminalNode* PrestoSqlParser::DropColumnContext::EXISTS(size_t i) {
  return getToken(PrestoSqlParser::EXISTS, i);
}

PrestoSqlParser::DropColumnContext::DropColumnContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DropColumnContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDropColumn(this);
}
void PrestoSqlParser::DropColumnContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDropColumn(this);
}

antlrcpp::Any PrestoSqlParser::DropColumnContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDropColumn(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DropViewContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DropViewContext::DROP() {
  return getToken(PrestoSqlParser::DROP, 0);
}

tree::TerminalNode* PrestoSqlParser::DropViewContext::VIEW() {
  return getToken(PrestoSqlParser::VIEW, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::DropViewContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::DropViewContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::DropViewContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

PrestoSqlParser::DropViewContext::DropViewContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DropViewContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDropView(this);
}
void PrestoSqlParser::DropViewContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDropView(this);
}

antlrcpp::Any PrestoSqlParser::DropViewContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDropView(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowTablesContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowTablesContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowTablesContext::TABLES() {
  return getToken(PrestoSqlParser::TABLES, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::ShowTablesContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ShowTablesContext::LIKE() {
  return getToken(PrestoSqlParser::LIKE, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowTablesContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowTablesContext::IN() {
  return getToken(PrestoSqlParser::IN, 0);
}

std::vector<PrestoSqlParser::StringContext*>
PrestoSqlParser::ShowTablesContext::string() {
  return getRuleContexts<PrestoSqlParser::StringContext>();
}

PrestoSqlParser::StringContext* PrestoSqlParser::ShowTablesContext::string(
    size_t i) {
  return getRuleContext<PrestoSqlParser::StringContext>(i);
}

tree::TerminalNode* PrestoSqlParser::ShowTablesContext::ESCAPE() {
  return getToken(PrestoSqlParser::ESCAPE, 0);
}

PrestoSqlParser::ShowTablesContext::ShowTablesContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowTablesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowTables(this);
}
void PrestoSqlParser::ShowTablesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowTables(this);
}

antlrcpp::Any PrestoSqlParser::ShowTablesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowTables(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowCatalogsContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowCatalogsContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowCatalogsContext::CATALOGS() {
  return getToken(PrestoSqlParser::CATALOGS, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowCatalogsContext::LIKE() {
  return getToken(PrestoSqlParser::LIKE, 0);
}

std::vector<PrestoSqlParser::StringContext*>
PrestoSqlParser::ShowCatalogsContext::string() {
  return getRuleContexts<PrestoSqlParser::StringContext>();
}

PrestoSqlParser::StringContext* PrestoSqlParser::ShowCatalogsContext::string(
    size_t i) {
  return getRuleContext<PrestoSqlParser::StringContext>(i);
}

tree::TerminalNode* PrestoSqlParser::ShowCatalogsContext::ESCAPE() {
  return getToken(PrestoSqlParser::ESCAPE, 0);
}

PrestoSqlParser::ShowCatalogsContext::ShowCatalogsContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowCatalogsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowCatalogs(this);
}
void PrestoSqlParser::ShowCatalogsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowCatalogs(this);
}

antlrcpp::Any PrestoSqlParser::ShowCatalogsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowCatalogs(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowRolesContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowRolesContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowRolesContext::ROLES() {
  return getToken(PrestoSqlParser::ROLES, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowRolesContext::CURRENT() {
  return getToken(PrestoSqlParser::CURRENT, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ShowRolesContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ShowRolesContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowRolesContext::IN() {
  return getToken(PrestoSqlParser::IN, 0);
}

PrestoSqlParser::ShowRolesContext::ShowRolesContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowRolesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowRoles(this);
}
void PrestoSqlParser::ShowRolesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowRoles(this);
}

antlrcpp::Any PrestoSqlParser::ShowRolesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowRoles(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RenameColumnContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RenameColumnContext::ALTER() {
  return getToken(PrestoSqlParser::ALTER, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameColumnContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameColumnContext::RENAME() {
  return getToken(PrestoSqlParser::RENAME, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameColumnContext::COLUMN() {
  return getToken(PrestoSqlParser::COLUMN, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameColumnContext::TO() {
  return getToken(PrestoSqlParser::TO, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::RenameColumnContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

std::vector<PrestoSqlParser::IdentifierContext*>
PrestoSqlParser::RenameColumnContext::identifier() {
  return getRuleContexts<PrestoSqlParser::IdentifierContext>();
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::RenameColumnContext::identifier(size_t i) {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(i);
}

std::vector<tree::TerminalNode*> PrestoSqlParser::RenameColumnContext::IF() {
  return getTokens(PrestoSqlParser::IF);
}

tree::TerminalNode* PrestoSqlParser::RenameColumnContext::IF(size_t i) {
  return getToken(PrestoSqlParser::IF, i);
}

std::vector<tree::TerminalNode*>
PrestoSqlParser::RenameColumnContext::EXISTS() {
  return getTokens(PrestoSqlParser::EXISTS);
}

tree::TerminalNode* PrestoSqlParser::RenameColumnContext::EXISTS(size_t i) {
  return getToken(PrestoSqlParser::EXISTS, i);
}

PrestoSqlParser::RenameColumnContext::RenameColumnContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RenameColumnContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRenameColumn(this);
}
void PrestoSqlParser::RenameColumnContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRenameColumn(this);
}

antlrcpp::Any PrestoSqlParser::RenameColumnContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRenameColumn(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RevokeRolesContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RevokeRolesContext::REVOKE() {
  return getToken(PrestoSqlParser::REVOKE, 0);
}

PrestoSqlParser::RolesContext* PrestoSqlParser::RevokeRolesContext::roles() {
  return getRuleContext<PrestoSqlParser::RolesContext>(0);
}

tree::TerminalNode* PrestoSqlParser::RevokeRolesContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

std::vector<PrestoSqlParser::PrincipalContext*>
PrestoSqlParser::RevokeRolesContext::principal() {
  return getRuleContexts<PrestoSqlParser::PrincipalContext>();
}

PrestoSqlParser::PrincipalContext*
PrestoSqlParser::RevokeRolesContext::principal(size_t i) {
  return getRuleContext<PrestoSqlParser::PrincipalContext>(i);
}

tree::TerminalNode* PrestoSqlParser::RevokeRolesContext::ADMIN() {
  return getToken(PrestoSqlParser::ADMIN, 0);
}

tree::TerminalNode* PrestoSqlParser::RevokeRolesContext::OPTION() {
  return getToken(PrestoSqlParser::OPTION, 0);
}

tree::TerminalNode* PrestoSqlParser::RevokeRolesContext::FOR() {
  return getToken(PrestoSqlParser::FOR, 0);
}

tree::TerminalNode* PrestoSqlParser::RevokeRolesContext::GRANTED() {
  return getToken(PrestoSqlParser::GRANTED, 0);
}

tree::TerminalNode* PrestoSqlParser::RevokeRolesContext::BY() {
  return getToken(PrestoSqlParser::BY, 0);
}

PrestoSqlParser::GrantorContext*
PrestoSqlParser::RevokeRolesContext::grantor() {
  return getRuleContext<PrestoSqlParser::GrantorContext>(0);
}

PrestoSqlParser::RevokeRolesContext::RevokeRolesContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RevokeRolesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRevokeRoles(this);
}
void PrestoSqlParser::RevokeRolesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRevokeRoles(this);
}

antlrcpp::Any PrestoSqlParser::RevokeRolesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRevokeRoles(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowCreateTableContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowCreateTableContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowCreateTableContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowCreateTableContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::ShowCreateTableContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::ShowCreateTableContext::ShowCreateTableContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowCreateTableContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowCreateTable(this);
}
void PrestoSqlParser::ShowCreateTableContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowCreateTable(this);
}

antlrcpp::Any PrestoSqlParser::ShowCreateTableContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowCreateTable(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowColumnsContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowColumnsContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowColumnsContext::COLUMNS() {
  return getToken(PrestoSqlParser::COLUMNS, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::ShowColumnsContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ShowColumnsContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowColumnsContext::IN() {
  return getToken(PrestoSqlParser::IN, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowColumnsContext::DESCRIBE() {
  return getToken(PrestoSqlParser::DESCRIBE, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowColumnsContext::DESC() {
  return getToken(PrestoSqlParser::DESC, 0);
}

PrestoSqlParser::ShowColumnsContext::ShowColumnsContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowColumnsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowColumns(this);
}
void PrestoSqlParser::ShowColumnsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowColumns(this);
}

antlrcpp::Any PrestoSqlParser::ShowColumnsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowColumns(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowRoleGrantsContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowRoleGrantsContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowRoleGrantsContext::ROLE() {
  return getToken(PrestoSqlParser::ROLE, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowRoleGrantsContext::GRANTS() {
  return getToken(PrestoSqlParser::GRANTS, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ShowRoleGrantsContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ShowRoleGrantsContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowRoleGrantsContext::IN() {
  return getToken(PrestoSqlParser::IN, 0);
}

PrestoSqlParser::ShowRoleGrantsContext::ShowRoleGrantsContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowRoleGrantsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowRoleGrants(this);
}
void PrestoSqlParser::ShowRoleGrantsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowRoleGrants(this);
}

antlrcpp::Any PrestoSqlParser::ShowRoleGrantsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowRoleGrants(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AddColumnContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::AddColumnContext::ALTER() {
  return getToken(PrestoSqlParser::ALTER, 0);
}

tree::TerminalNode* PrestoSqlParser::AddColumnContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::AddColumnContext::ADD() {
  return getToken(PrestoSqlParser::ADD, 0);
}

tree::TerminalNode* PrestoSqlParser::AddColumnContext::COLUMN() {
  return getToken(PrestoSqlParser::COLUMN, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::AddColumnContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::ColumnDefinitionContext*
PrestoSqlParser::AddColumnContext::columnDefinition() {
  return getRuleContext<PrestoSqlParser::ColumnDefinitionContext>(0);
}

std::vector<tree::TerminalNode*> PrestoSqlParser::AddColumnContext::IF() {
  return getTokens(PrestoSqlParser::IF);
}

tree::TerminalNode* PrestoSqlParser::AddColumnContext::IF(size_t i) {
  return getToken(PrestoSqlParser::IF, i);
}

std::vector<tree::TerminalNode*> PrestoSqlParser::AddColumnContext::EXISTS() {
  return getTokens(PrestoSqlParser::EXISTS);
}

tree::TerminalNode* PrestoSqlParser::AddColumnContext::EXISTS(size_t i) {
  return getToken(PrestoSqlParser::EXISTS, i);
}

tree::TerminalNode* PrestoSqlParser::AddColumnContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

PrestoSqlParser::AddColumnContext::AddColumnContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::AddColumnContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAddColumn(this);
}
void PrestoSqlParser::AddColumnContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitAddColumn(this);
}

antlrcpp::Any PrestoSqlParser::AddColumnContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAddColumn(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ResetSessionContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ResetSessionContext::RESET() {
  return getToken(PrestoSqlParser::RESET, 0);
}

tree::TerminalNode* PrestoSqlParser::ResetSessionContext::SESSION() {
  return getToken(PrestoSqlParser::SESSION, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::ResetSessionContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::ResetSessionContext::ResetSessionContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ResetSessionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterResetSession(this);
}
void PrestoSqlParser::ResetSessionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitResetSession(this);
}

antlrcpp::Any PrestoSqlParser::ResetSessionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitResetSession(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DropConstraintContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DropConstraintContext::ALTER() {
  return getToken(PrestoSqlParser::ALTER, 0);
}

tree::TerminalNode* PrestoSqlParser::DropConstraintContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::DropConstraintContext::DROP() {
  return getToken(PrestoSqlParser::DROP, 0);
}

tree::TerminalNode* PrestoSqlParser::DropConstraintContext::CONSTRAINT() {
  return getToken(PrestoSqlParser::CONSTRAINT, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::DropConstraintContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::DropConstraintContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

std::vector<tree::TerminalNode*> PrestoSqlParser::DropConstraintContext::IF() {
  return getTokens(PrestoSqlParser::IF);
}

tree::TerminalNode* PrestoSqlParser::DropConstraintContext::IF(size_t i) {
  return getToken(PrestoSqlParser::IF, i);
}

std::vector<tree::TerminalNode*>
PrestoSqlParser::DropConstraintContext::EXISTS() {
  return getTokens(PrestoSqlParser::EXISTS);
}

tree::TerminalNode* PrestoSqlParser::DropConstraintContext::EXISTS(size_t i) {
  return getToken(PrestoSqlParser::EXISTS, i);
}

PrestoSqlParser::DropConstraintContext::DropConstraintContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DropConstraintContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDropConstraint(this);
}
void PrestoSqlParser::DropConstraintContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDropConstraint(this);
}

antlrcpp::Any PrestoSqlParser::DropConstraintContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDropConstraint(this);
  else
    return visitor->visitChildren(this);
}
//----------------- InsertIntoContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::InsertIntoContext::INSERT() {
  return getToken(PrestoSqlParser::INSERT, 0);
}

tree::TerminalNode* PrestoSqlParser::InsertIntoContext::INTO() {
  return getToken(PrestoSqlParser::INTO, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::InsertIntoContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::QueryContext* PrestoSqlParser::InsertIntoContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

PrestoSqlParser::ColumnAliasesContext*
PrestoSqlParser::InsertIntoContext::columnAliases() {
  return getRuleContext<PrestoSqlParser::ColumnAliasesContext>(0);
}

PrestoSqlParser::InsertIntoContext::InsertIntoContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::InsertIntoContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterInsertInto(this);
}
void PrestoSqlParser::InsertIntoContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitInsertInto(this);
}

antlrcpp::Any PrestoSqlParser::InsertIntoContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitInsertInto(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowSessionContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowSessionContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowSessionContext::SESSION() {
  return getToken(PrestoSqlParser::SESSION, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowSessionContext::LIKE() {
  return getToken(PrestoSqlParser::LIKE, 0);
}

std::vector<PrestoSqlParser::StringContext*>
PrestoSqlParser::ShowSessionContext::string() {
  return getRuleContexts<PrestoSqlParser::StringContext>();
}

PrestoSqlParser::StringContext* PrestoSqlParser::ShowSessionContext::string(
    size_t i) {
  return getRuleContext<PrestoSqlParser::StringContext>(i);
}

tree::TerminalNode* PrestoSqlParser::ShowSessionContext::ESCAPE() {
  return getToken(PrestoSqlParser::ESCAPE, 0);
}

PrestoSqlParser::ShowSessionContext::ShowSessionContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowSessionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowSession(this);
}
void PrestoSqlParser::ShowSessionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowSession(this);
}

antlrcpp::Any PrestoSqlParser::ShowSessionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowSession(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateSchemaContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CreateSchemaContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateSchemaContext::SCHEMA() {
  return getToken(PrestoSqlParser::SCHEMA, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::CreateSchemaContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateSchemaContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateSchemaContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateSchemaContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateSchemaContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

PrestoSqlParser::PropertiesContext*
PrestoSqlParser::CreateSchemaContext::properties() {
  return getRuleContext<PrestoSqlParser::PropertiesContext>(0);
}

PrestoSqlParser::CreateSchemaContext::CreateSchemaContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CreateSchemaContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCreateSchema(this);
}
void PrestoSqlParser::CreateSchemaContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCreateSchema(this);
}

antlrcpp::Any PrestoSqlParser::CreateSchemaContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCreateSchema(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ExecuteContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ExecuteContext::EXECUTE() {
  return getToken(PrestoSqlParser::EXECUTE, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ExecuteContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ExecuteContext::USING() {
  return getToken(PrestoSqlParser::USING, 0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::ExecuteContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext* PrestoSqlParser::ExecuteContext::expression(
    size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

PrestoSqlParser::ExecuteContext::ExecuteContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ExecuteContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterExecute(this);
}
void PrestoSqlParser::ExecuteContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitExecute(this);
}

antlrcpp::Any PrestoSqlParser::ExecuteContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExecute(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RenameSchemaContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RenameSchemaContext::ALTER() {
  return getToken(PrestoSqlParser::ALTER, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameSchemaContext::SCHEMA() {
  return getToken(PrestoSqlParser::SCHEMA, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::RenameSchemaContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::RenameSchemaContext::RENAME() {
  return getToken(PrestoSqlParser::RENAME, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameSchemaContext::TO() {
  return getToken(PrestoSqlParser::TO, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::RenameSchemaContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::RenameSchemaContext::RenameSchemaContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RenameSchemaContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRenameSchema(this);
}
void PrestoSqlParser::RenameSchemaContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRenameSchema(this);
}

antlrcpp::Any PrestoSqlParser::RenameSchemaContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRenameSchema(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DropRoleContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DropRoleContext::DROP() {
  return getToken(PrestoSqlParser::DROP, 0);
}

tree::TerminalNode* PrestoSqlParser::DropRoleContext::ROLE() {
  return getToken(PrestoSqlParser::ROLE, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::DropRoleContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::DropRoleContext::DropRoleContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DropRoleContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDropRole(this);
}
void PrestoSqlParser::DropRoleContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDropRole(this);
}

antlrcpp::Any PrestoSqlParser::DropRoleContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDropRole(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AnalyzeContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::AnalyzeContext::ANALYZE() {
  return getToken(PrestoSqlParser::ANALYZE, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::AnalyzeContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::AnalyzeContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

PrestoSqlParser::PropertiesContext*
PrestoSqlParser::AnalyzeContext::properties() {
  return getRuleContext<PrestoSqlParser::PropertiesContext>(0);
}

PrestoSqlParser::AnalyzeContext::AnalyzeContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::AnalyzeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAnalyze(this);
}
void PrestoSqlParser::AnalyzeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitAnalyze(this);
}

antlrcpp::Any PrestoSqlParser::AnalyzeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAnalyze(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SetRoleContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::SetRoleContext::SET() {
  return getToken(PrestoSqlParser::SET, 0);
}

tree::TerminalNode* PrestoSqlParser::SetRoleContext::ROLE() {
  return getToken(PrestoSqlParser::ROLE, 0);
}

tree::TerminalNode* PrestoSqlParser::SetRoleContext::ALL() {
  return getToken(PrestoSqlParser::ALL, 0);
}

tree::TerminalNode* PrestoSqlParser::SetRoleContext::NONE() {
  return getToken(PrestoSqlParser::NONE, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::SetRoleContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::SetRoleContext::SetRoleContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SetRoleContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSetRole(this);
}
void PrestoSqlParser::SetRoleContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSetRole(this);
}

antlrcpp::Any PrestoSqlParser::SetRoleContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSetRole(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateFunctionContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CreateFunctionContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateFunctionContext::FUNCTION() {
  return getToken(PrestoSqlParser::FUNCTION, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateFunctionContext::RETURNS() {
  return getToken(PrestoSqlParser::RETURNS, 0);
}

PrestoSqlParser::RoutineCharacteristicsContext*
PrestoSqlParser::CreateFunctionContext::routineCharacteristics() {
  return getRuleContext<PrestoSqlParser::RoutineCharacteristicsContext>(0);
}

PrestoSqlParser::RoutineBodyContext*
PrestoSqlParser::CreateFunctionContext::routineBody() {
  return getRuleContext<PrestoSqlParser::RoutineBodyContext>(0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::CreateFunctionContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::TypeContext* PrestoSqlParser::CreateFunctionContext::type() {
  return getRuleContext<PrestoSqlParser::TypeContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateFunctionContext::OR() {
  return getToken(PrestoSqlParser::OR, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateFunctionContext::REPLACE() {
  return getToken(PrestoSqlParser::REPLACE, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateFunctionContext::TEMPORARY() {
  return getToken(PrestoSqlParser::TEMPORARY, 0);
}

std::vector<PrestoSqlParser::SqlParameterDeclarationContext*>
PrestoSqlParser::CreateFunctionContext::sqlParameterDeclaration() {
  return getRuleContexts<PrestoSqlParser::SqlParameterDeclarationContext>();
}

PrestoSqlParser::SqlParameterDeclarationContext*
PrestoSqlParser::CreateFunctionContext::sqlParameterDeclaration(size_t i) {
  return getRuleContext<PrestoSqlParser::SqlParameterDeclarationContext>(i);
}

tree::TerminalNode* PrestoSqlParser::CreateFunctionContext::COMMENT() {
  return getToken(PrestoSqlParser::COMMENT, 0);
}

PrestoSqlParser::StringContext*
PrestoSqlParser::CreateFunctionContext::string() {
  return getRuleContext<PrestoSqlParser::StringContext>(0);
}

PrestoSqlParser::CreateFunctionContext::CreateFunctionContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CreateFunctionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCreateFunction(this);
}
void PrestoSqlParser::CreateFunctionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCreateFunction(this);
}

antlrcpp::Any PrestoSqlParser::CreateFunctionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCreateFunction(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowGrantsContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowGrantsContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowGrantsContext::GRANTS() {
  return getToken(PrestoSqlParser::GRANTS, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowGrantsContext::ON() {
  return getToken(PrestoSqlParser::ON, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::ShowGrantsContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ShowGrantsContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

PrestoSqlParser::ShowGrantsContext::ShowGrantsContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowGrantsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowGrants(this);
}
void PrestoSqlParser::ShowGrantsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowGrants(this);
}

antlrcpp::Any PrestoSqlParser::ShowGrantsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowGrants(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DropSchemaContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DropSchemaContext::DROP() {
  return getToken(PrestoSqlParser::DROP, 0);
}

tree::TerminalNode* PrestoSqlParser::DropSchemaContext::SCHEMA() {
  return getToken(PrestoSqlParser::SCHEMA, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::DropSchemaContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::DropSchemaContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::DropSchemaContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

tree::TerminalNode* PrestoSqlParser::DropSchemaContext::CASCADE() {
  return getToken(PrestoSqlParser::CASCADE, 0);
}

tree::TerminalNode* PrestoSqlParser::DropSchemaContext::RESTRICT() {
  return getToken(PrestoSqlParser::RESTRICT, 0);
}

PrestoSqlParser::DropSchemaContext::DropSchemaContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DropSchemaContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDropSchema(this);
}
void PrestoSqlParser::DropSchemaContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDropSchema(this);
}

antlrcpp::Any PrestoSqlParser::DropSchemaContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDropSchema(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowCreateViewContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowCreateViewContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowCreateViewContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowCreateViewContext::VIEW() {
  return getToken(PrestoSqlParser::VIEW, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::ShowCreateViewContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::ShowCreateViewContext::ShowCreateViewContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowCreateViewContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowCreateView(this);
}
void PrestoSqlParser::ShowCreateViewContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowCreateView(this);
}

antlrcpp::Any PrestoSqlParser::ShowCreateViewContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowCreateView(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateTableContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CreateTableContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::CreateTableContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

std::vector<PrestoSqlParser::TableElementContext*>
PrestoSqlParser::CreateTableContext::tableElement() {
  return getRuleContexts<PrestoSqlParser::TableElementContext>();
}

PrestoSqlParser::TableElementContext*
PrestoSqlParser::CreateTableContext::tableElement(size_t i) {
  return getRuleContext<PrestoSqlParser::TableElementContext>(i);
}

tree::TerminalNode* PrestoSqlParser::CreateTableContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableContext::COMMENT() {
  return getToken(PrestoSqlParser::COMMENT, 0);
}

PrestoSqlParser::StringContext* PrestoSqlParser::CreateTableContext::string() {
  return getRuleContext<PrestoSqlParser::StringContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

PrestoSqlParser::PropertiesContext*
PrestoSqlParser::CreateTableContext::properties() {
  return getRuleContext<PrestoSqlParser::PropertiesContext>(0);
}

PrestoSqlParser::CreateTableContext::CreateTableContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CreateTableContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCreateTable(this);
}
void PrestoSqlParser::CreateTableContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCreateTable(this);
}

antlrcpp::Any PrestoSqlParser::CreateTableContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCreateTable(this);
  else
    return visitor->visitChildren(this);
}
//----------------- StartTransactionContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::StartTransactionContext::START() {
  return getToken(PrestoSqlParser::START, 0);
}

tree::TerminalNode* PrestoSqlParser::StartTransactionContext::TRANSACTION() {
  return getToken(PrestoSqlParser::TRANSACTION, 0);
}

std::vector<PrestoSqlParser::TransactionModeContext*>
PrestoSqlParser::StartTransactionContext::transactionMode() {
  return getRuleContexts<PrestoSqlParser::TransactionModeContext>();
}

PrestoSqlParser::TransactionModeContext*
PrestoSqlParser::StartTransactionContext::transactionMode(size_t i) {
  return getRuleContext<PrestoSqlParser::TransactionModeContext>(i);
}

PrestoSqlParser::StartTransactionContext::StartTransactionContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::StartTransactionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterStartTransaction(this);
}
void PrestoSqlParser::StartTransactionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitStartTransaction(this);
}

antlrcpp::Any PrestoSqlParser::StartTransactionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitStartTransaction(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateTableAsSelectContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CreateTableAsSelectContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableAsSelectContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::CreateTableAsSelectContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableAsSelectContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

PrestoSqlParser::QueryContext*
PrestoSqlParser::CreateTableAsSelectContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableAsSelectContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableAsSelectContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableAsSelectContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

PrestoSqlParser::ColumnAliasesContext*
PrestoSqlParser::CreateTableAsSelectContext::columnAliases() {
  return getRuleContext<PrestoSqlParser::ColumnAliasesContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableAsSelectContext::COMMENT() {
  return getToken(PrestoSqlParser::COMMENT, 0);
}

PrestoSqlParser::StringContext*
PrestoSqlParser::CreateTableAsSelectContext::string() {
  return getRuleContext<PrestoSqlParser::StringContext>(0);
}

std::vector<tree::TerminalNode*>
PrestoSqlParser::CreateTableAsSelectContext::WITH() {
  return getTokens(PrestoSqlParser::WITH);
}

tree::TerminalNode* PrestoSqlParser::CreateTableAsSelectContext::WITH(
    size_t i) {
  return getToken(PrestoSqlParser::WITH, i);
}

PrestoSqlParser::PropertiesContext*
PrestoSqlParser::CreateTableAsSelectContext::properties() {
  return getRuleContext<PrestoSqlParser::PropertiesContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableAsSelectContext::DATA() {
  return getToken(PrestoSqlParser::DATA, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateTableAsSelectContext::NO() {
  return getToken(PrestoSqlParser::NO, 0);
}

PrestoSqlParser::CreateTableAsSelectContext::CreateTableAsSelectContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CreateTableAsSelectContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCreateTableAsSelect(this);
}
void PrestoSqlParser::CreateTableAsSelectContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCreateTableAsSelect(this);
}

antlrcpp::Any PrestoSqlParser::CreateTableAsSelectContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCreateTableAsSelect(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowStatsContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowStatsContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowStatsContext::STATS() {
  return getToken(PrestoSqlParser::STATS, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowStatsContext::FOR() {
  return getToken(PrestoSqlParser::FOR, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::ShowStatsContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::ShowStatsContext::ShowStatsContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowStatsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowStats(this);
}
void PrestoSqlParser::ShowStatsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowStats(this);
}

antlrcpp::Any PrestoSqlParser::ShowStatsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowStats(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DropFunctionContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DropFunctionContext::DROP() {
  return getToken(PrestoSqlParser::DROP, 0);
}

tree::TerminalNode* PrestoSqlParser::DropFunctionContext::FUNCTION() {
  return getToken(PrestoSqlParser::FUNCTION, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::DropFunctionContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::DropFunctionContext::TEMPORARY() {
  return getToken(PrestoSqlParser::TEMPORARY, 0);
}

tree::TerminalNode* PrestoSqlParser::DropFunctionContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::DropFunctionContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

PrestoSqlParser::TypesContext* PrestoSqlParser::DropFunctionContext::types() {
  return getRuleContext<PrestoSqlParser::TypesContext>(0);
}

PrestoSqlParser::DropFunctionContext::DropFunctionContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DropFunctionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDropFunction(this);
}
void PrestoSqlParser::DropFunctionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDropFunction(this);
}

antlrcpp::Any PrestoSqlParser::DropFunctionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDropFunction(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RevokeContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RevokeContext::REVOKE() {
  return getToken(PrestoSqlParser::REVOKE, 0);
}

tree::TerminalNode* PrestoSqlParser::RevokeContext::ON() {
  return getToken(PrestoSqlParser::ON, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::RevokeContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::RevokeContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

PrestoSqlParser::PrincipalContext* PrestoSqlParser::RevokeContext::principal() {
  return getRuleContext<PrestoSqlParser::PrincipalContext>(0);
}

std::vector<PrestoSqlParser::PrivilegeContext*>
PrestoSqlParser::RevokeContext::privilege() {
  return getRuleContexts<PrestoSqlParser::PrivilegeContext>();
}

PrestoSqlParser::PrivilegeContext* PrestoSqlParser::RevokeContext::privilege(
    size_t i) {
  return getRuleContext<PrestoSqlParser::PrivilegeContext>(i);
}

tree::TerminalNode* PrestoSqlParser::RevokeContext::ALL() {
  return getToken(PrestoSqlParser::ALL, 0);
}

tree::TerminalNode* PrestoSqlParser::RevokeContext::PRIVILEGES() {
  return getToken(PrestoSqlParser::PRIVILEGES, 0);
}

tree::TerminalNode* PrestoSqlParser::RevokeContext::GRANT() {
  return getToken(PrestoSqlParser::GRANT, 0);
}

tree::TerminalNode* PrestoSqlParser::RevokeContext::OPTION() {
  return getToken(PrestoSqlParser::OPTION, 0);
}

tree::TerminalNode* PrestoSqlParser::RevokeContext::FOR() {
  return getToken(PrestoSqlParser::FOR, 0);
}

tree::TerminalNode* PrestoSqlParser::RevokeContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

PrestoSqlParser::RevokeContext::RevokeContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RevokeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRevoke(this);
}
void PrestoSqlParser::RevokeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRevoke(this);
}

antlrcpp::Any PrestoSqlParser::RevokeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRevoke(this);
  else
    return visitor->visitChildren(this);
}
//----------------- UpdateContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::UpdateContext::UPDATE() {
  return getToken(PrestoSqlParser::UPDATE, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::UpdateContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::UpdateContext::SET() {
  return getToken(PrestoSqlParser::SET, 0);
}

std::vector<PrestoSqlParser::UpdateAssignmentContext*>
PrestoSqlParser::UpdateContext::updateAssignment() {
  return getRuleContexts<PrestoSqlParser::UpdateAssignmentContext>();
}

PrestoSqlParser::UpdateAssignmentContext*
PrestoSqlParser::UpdateContext::updateAssignment(size_t i) {
  return getRuleContext<PrestoSqlParser::UpdateAssignmentContext>(i);
}

tree::TerminalNode* PrestoSqlParser::UpdateContext::WHERE() {
  return getToken(PrestoSqlParser::WHERE, 0);
}

PrestoSqlParser::BooleanExpressionContext*
PrestoSqlParser::UpdateContext::booleanExpression() {
  return getRuleContext<PrestoSqlParser::BooleanExpressionContext>(0);
}

PrestoSqlParser::UpdateContext::UpdateContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::UpdateContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterUpdate(this);
}
void PrestoSqlParser::UpdateContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitUpdate(this);
}

antlrcpp::Any PrestoSqlParser::UpdateContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUpdate(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateTypeContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CreateTypeContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateTypeContext::TYPE() {
  return getToken(PrestoSqlParser::TYPE, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::CreateTypeContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateTypeContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

std::vector<PrestoSqlParser::SqlParameterDeclarationContext*>
PrestoSqlParser::CreateTypeContext::sqlParameterDeclaration() {
  return getRuleContexts<PrestoSqlParser::SqlParameterDeclarationContext>();
}

PrestoSqlParser::SqlParameterDeclarationContext*
PrestoSqlParser::CreateTypeContext::sqlParameterDeclaration(size_t i) {
  return getRuleContext<PrestoSqlParser::SqlParameterDeclarationContext>(i);
}

PrestoSqlParser::TypeContext* PrestoSqlParser::CreateTypeContext::type() {
  return getRuleContext<PrestoSqlParser::TypeContext>(0);
}

PrestoSqlParser::CreateTypeContext::CreateTypeContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CreateTypeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCreateType(this);
}
void PrestoSqlParser::CreateTypeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCreateType(this);
}

antlrcpp::Any PrestoSqlParser::CreateTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCreateType(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DeleteContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DeleteContext::DELETE() {
  return getToken(PrestoSqlParser::DELETE, 0);
}

tree::TerminalNode* PrestoSqlParser::DeleteContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::DeleteContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::DeleteContext::WHERE() {
  return getToken(PrestoSqlParser::WHERE, 0);
}

PrestoSqlParser::BooleanExpressionContext*
PrestoSqlParser::DeleteContext::booleanExpression() {
  return getRuleContext<PrestoSqlParser::BooleanExpressionContext>(0);
}

PrestoSqlParser::DeleteContext::DeleteContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DeleteContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDelete(this);
}
void PrestoSqlParser::DeleteContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDelete(this);
}

antlrcpp::Any PrestoSqlParser::DeleteContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDelete(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DescribeInputContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DescribeInputContext::DESCRIBE() {
  return getToken(PrestoSqlParser::DESCRIBE, 0);
}

tree::TerminalNode* PrestoSqlParser::DescribeInputContext::INPUT() {
  return getToken(PrestoSqlParser::INPUT, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::DescribeInputContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::DescribeInputContext::DescribeInputContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DescribeInputContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDescribeInput(this);
}
void PrestoSqlParser::DescribeInputContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDescribeInput(this);
}

antlrcpp::Any PrestoSqlParser::DescribeInputContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDescribeInput(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowStatsForQueryContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowStatsForQueryContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowStatsForQueryContext::STATS() {
  return getToken(PrestoSqlParser::STATS, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowStatsForQueryContext::FOR() {
  return getToken(PrestoSqlParser::FOR, 0);
}

PrestoSqlParser::QuerySpecificationContext*
PrestoSqlParser::ShowStatsForQueryContext::querySpecification() {
  return getRuleContext<PrestoSqlParser::QuerySpecificationContext>(0);
}

PrestoSqlParser::ShowStatsForQueryContext::ShowStatsForQueryContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowStatsForQueryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowStatsForQuery(this);
}
void PrestoSqlParser::ShowStatsForQueryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowStatsForQuery(this);
}

antlrcpp::Any PrestoSqlParser::ShowStatsForQueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowStatsForQuery(this);
  else
    return visitor->visitChildren(this);
}
//----------------- StatementDefaultContext
//------------------------------------------------------------------

PrestoSqlParser::QueryContext*
PrestoSqlParser::StatementDefaultContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

PrestoSqlParser::StatementDefaultContext::StatementDefaultContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::StatementDefaultContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterStatementDefault(this);
}
void PrestoSqlParser::StatementDefaultContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitStatementDefault(this);
}

antlrcpp::Any PrestoSqlParser::StatementDefaultContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitStatementDefault(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TruncateTableContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::TruncateTableContext::TRUNCATE() {
  return getToken(PrestoSqlParser::TRUNCATE, 0);
}

tree::TerminalNode* PrestoSqlParser::TruncateTableContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::TruncateTableContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::TruncateTableContext::TruncateTableContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::TruncateTableContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTruncateTable(this);
}
void PrestoSqlParser::TruncateTableContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTruncateTable(this);
}

antlrcpp::Any PrestoSqlParser::TruncateTableContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTruncateTable(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterColumnSetNotNullContext
//------------------------------------------------------------------

std::vector<tree::TerminalNode*>
PrestoSqlParser::AlterColumnSetNotNullContext::ALTER() {
  return getTokens(PrestoSqlParser::ALTER);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnSetNotNullContext::ALTER(
    size_t i) {
  return getToken(PrestoSqlParser::ALTER, i);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnSetNotNullContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnSetNotNullContext::SET() {
  return getToken(PrestoSqlParser::SET, 0);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnSetNotNullContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

tree::TerminalNode*
PrestoSqlParser::AlterColumnSetNotNullContext::NULL_LITERAL() {
  return getToken(PrestoSqlParser::NULL_LITERAL, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::AlterColumnSetNotNullContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::AlterColumnSetNotNullContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnSetNotNullContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnSetNotNullContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnSetNotNullContext::COLUMN() {
  return getToken(PrestoSqlParser::COLUMN, 0);
}

PrestoSqlParser::AlterColumnSetNotNullContext::AlterColumnSetNotNullContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::AlterColumnSetNotNullContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAlterColumnSetNotNull(this);
}
void PrestoSqlParser::AlterColumnSetNotNullContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitAlterColumnSetNotNull(this);
}

antlrcpp::Any PrestoSqlParser::AlterColumnSetNotNullContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAlterColumnSetNotNull(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateMaterializedViewContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CreateMaterializedViewContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode*
PrestoSqlParser::CreateMaterializedViewContext::MATERIALIZED() {
  return getToken(PrestoSqlParser::MATERIALIZED, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateMaterializedViewContext::VIEW() {
  return getToken(PrestoSqlParser::VIEW, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::CreateMaterializedViewContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateMaterializedViewContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

PrestoSqlParser::QueryContext*
PrestoSqlParser::CreateMaterializedViewContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateMaterializedViewContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateMaterializedViewContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateMaterializedViewContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateMaterializedViewContext::COMMENT() {
  return getToken(PrestoSqlParser::COMMENT, 0);
}

PrestoSqlParser::StringContext*
PrestoSqlParser::CreateMaterializedViewContext::string() {
  return getRuleContext<PrestoSqlParser::StringContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateMaterializedViewContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

PrestoSqlParser::PropertiesContext*
PrestoSqlParser::CreateMaterializedViewContext::properties() {
  return getRuleContext<PrestoSqlParser::PropertiesContext>(0);
}

PrestoSqlParser::CreateMaterializedViewContext::CreateMaterializedViewContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CreateMaterializedViewContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCreateMaterializedView(this);
}
void PrestoSqlParser::CreateMaterializedViewContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCreateMaterializedView(this);
}

antlrcpp::Any PrestoSqlParser::CreateMaterializedViewContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCreateMaterializedView(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterFunctionContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::AlterFunctionContext::ALTER() {
  return getToken(PrestoSqlParser::ALTER, 0);
}

tree::TerminalNode* PrestoSqlParser::AlterFunctionContext::FUNCTION() {
  return getToken(PrestoSqlParser::FUNCTION, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::AlterFunctionContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::AlterRoutineCharacteristicsContext*
PrestoSqlParser::AlterFunctionContext::alterRoutineCharacteristics() {
  return getRuleContext<PrestoSqlParser::AlterRoutineCharacteristicsContext>(0);
}

PrestoSqlParser::TypesContext* PrestoSqlParser::AlterFunctionContext::types() {
  return getRuleContext<PrestoSqlParser::TypesContext>(0);
}

PrestoSqlParser::AlterFunctionContext::AlterFunctionContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::AlterFunctionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAlterFunction(this);
}
void PrestoSqlParser::AlterFunctionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitAlterFunction(this);
}

antlrcpp::Any PrestoSqlParser::AlterFunctionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAlterFunction(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SetSessionContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::SetSessionContext::SET() {
  return getToken(PrestoSqlParser::SET, 0);
}

tree::TerminalNode* PrestoSqlParser::SetSessionContext::SESSION() {
  return getToken(PrestoSqlParser::SESSION, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::SetSessionContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::SetSessionContext::EQ() {
  return getToken(PrestoSqlParser::EQ, 0);
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::SetSessionContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

PrestoSqlParser::SetSessionContext::SetSessionContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SetSessionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSetSession(this);
}
void PrestoSqlParser::SetSessionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSetSession(this);
}

antlrcpp::Any PrestoSqlParser::SetSessionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSetSession(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CreateViewContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CreateViewContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateViewContext::VIEW() {
  return getToken(PrestoSqlParser::VIEW, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::CreateViewContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateViewContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

PrestoSqlParser::QueryContext* PrestoSqlParser::CreateViewContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CreateViewContext::OR() {
  return getToken(PrestoSqlParser::OR, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateViewContext::REPLACE() {
  return getToken(PrestoSqlParser::REPLACE, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateViewContext::SECURITY() {
  return getToken(PrestoSqlParser::SECURITY, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateViewContext::DEFINER() {
  return getToken(PrestoSqlParser::DEFINER, 0);
}

tree::TerminalNode* PrestoSqlParser::CreateViewContext::INVOKER() {
  return getToken(PrestoSqlParser::INVOKER, 0);
}

PrestoSqlParser::CreateViewContext::CreateViewContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CreateViewContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCreateView(this);
}
void PrestoSqlParser::CreateViewContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCreateView(this);
}

antlrcpp::Any PrestoSqlParser::CreateViewContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCreateView(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowSchemasContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowSchemasContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowSchemasContext::SCHEMAS() {
  return getToken(PrestoSqlParser::SCHEMAS, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ShowSchemasContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ShowSchemasContext::LIKE() {
  return getToken(PrestoSqlParser::LIKE, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowSchemasContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowSchemasContext::IN() {
  return getToken(PrestoSqlParser::IN, 0);
}

std::vector<PrestoSqlParser::StringContext*>
PrestoSqlParser::ShowSchemasContext::string() {
  return getRuleContexts<PrestoSqlParser::StringContext>();
}

PrestoSqlParser::StringContext* PrestoSqlParser::ShowSchemasContext::string(
    size_t i) {
  return getRuleContext<PrestoSqlParser::StringContext>(i);
}

tree::TerminalNode* PrestoSqlParser::ShowSchemasContext::ESCAPE() {
  return getToken(PrestoSqlParser::ESCAPE, 0);
}

PrestoSqlParser::ShowSchemasContext::ShowSchemasContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowSchemasContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowSchemas(this);
}
void PrestoSqlParser::ShowSchemasContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowSchemas(this);
}

antlrcpp::Any PrestoSqlParser::ShowSchemasContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowSchemas(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DropTableContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DropTableContext::DROP() {
  return getToken(PrestoSqlParser::DROP, 0);
}

tree::TerminalNode* PrestoSqlParser::DropTableContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::DropTableContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::DropTableContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::DropTableContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

PrestoSqlParser::DropTableContext::DropTableContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DropTableContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDropTable(this);
}
void PrestoSqlParser::DropTableContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDropTable(this);
}

antlrcpp::Any PrestoSqlParser::DropTableContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDropTable(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RollbackContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RollbackContext::ROLLBACK() {
  return getToken(PrestoSqlParser::ROLLBACK, 0);
}

tree::TerminalNode* PrestoSqlParser::RollbackContext::WORK() {
  return getToken(PrestoSqlParser::WORK, 0);
}

PrestoSqlParser::RollbackContext::RollbackContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RollbackContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRollback(this);
}
void PrestoSqlParser::RollbackContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRollback(this);
}

antlrcpp::Any PrestoSqlParser::RollbackContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRollback(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RenameViewContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RenameViewContext::ALTER() {
  return getToken(PrestoSqlParser::ALTER, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameViewContext::VIEW() {
  return getToken(PrestoSqlParser::VIEW, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameViewContext::RENAME() {
  return getToken(PrestoSqlParser::RENAME, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameViewContext::TO() {
  return getToken(PrestoSqlParser::TO, 0);
}

std::vector<PrestoSqlParser::QualifiedNameContext*>
PrestoSqlParser::RenameViewContext::qualifiedName() {
  return getRuleContexts<PrestoSqlParser::QualifiedNameContext>();
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::RenameViewContext::qualifiedName(size_t i) {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(i);
}

tree::TerminalNode* PrestoSqlParser::RenameViewContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::RenameViewContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

PrestoSqlParser::RenameViewContext::RenameViewContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RenameViewContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRenameView(this);
}
void PrestoSqlParser::RenameViewContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRenameView(this);
}

antlrcpp::Any PrestoSqlParser::RenameViewContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRenameView(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AlterColumnDropNotNullContext
//------------------------------------------------------------------

std::vector<tree::TerminalNode*>
PrestoSqlParser::AlterColumnDropNotNullContext::ALTER() {
  return getTokens(PrestoSqlParser::ALTER);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnDropNotNullContext::ALTER(
    size_t i) {
  return getToken(PrestoSqlParser::ALTER, i);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnDropNotNullContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnDropNotNullContext::DROP() {
  return getToken(PrestoSqlParser::DROP, 0);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnDropNotNullContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

tree::TerminalNode*
PrestoSqlParser::AlterColumnDropNotNullContext::NULL_LITERAL() {
  return getToken(PrestoSqlParser::NULL_LITERAL, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::AlterColumnDropNotNullContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::AlterColumnDropNotNullContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnDropNotNullContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnDropNotNullContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

tree::TerminalNode* PrestoSqlParser::AlterColumnDropNotNullContext::COLUMN() {
  return getToken(PrestoSqlParser::COLUMN, 0);
}

PrestoSqlParser::AlterColumnDropNotNullContext::AlterColumnDropNotNullContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::AlterColumnDropNotNullContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAlterColumnDropNotNull(this);
}
void PrestoSqlParser::AlterColumnDropNotNullContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitAlterColumnDropNotNull(this);
}

antlrcpp::Any PrestoSqlParser::AlterColumnDropNotNullContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAlterColumnDropNotNull(this);
  else
    return visitor->visitChildren(this);
}
//----------------- GrantRolesContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::GrantRolesContext::GRANT() {
  return getToken(PrestoSqlParser::GRANT, 0);
}

PrestoSqlParser::RolesContext* PrestoSqlParser::GrantRolesContext::roles() {
  return getRuleContext<PrestoSqlParser::RolesContext>(0);
}

tree::TerminalNode* PrestoSqlParser::GrantRolesContext::TO() {
  return getToken(PrestoSqlParser::TO, 0);
}

std::vector<PrestoSqlParser::PrincipalContext*>
PrestoSqlParser::GrantRolesContext::principal() {
  return getRuleContexts<PrestoSqlParser::PrincipalContext>();
}

PrestoSqlParser::PrincipalContext*
PrestoSqlParser::GrantRolesContext::principal(size_t i) {
  return getRuleContext<PrestoSqlParser::PrincipalContext>(i);
}

tree::TerminalNode* PrestoSqlParser::GrantRolesContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

tree::TerminalNode* PrestoSqlParser::GrantRolesContext::ADMIN() {
  return getToken(PrestoSqlParser::ADMIN, 0);
}

tree::TerminalNode* PrestoSqlParser::GrantRolesContext::OPTION() {
  return getToken(PrestoSqlParser::OPTION, 0);
}

tree::TerminalNode* PrestoSqlParser::GrantRolesContext::GRANTED() {
  return getToken(PrestoSqlParser::GRANTED, 0);
}

tree::TerminalNode* PrestoSqlParser::GrantRolesContext::BY() {
  return getToken(PrestoSqlParser::BY, 0);
}

PrestoSqlParser::GrantorContext* PrestoSqlParser::GrantRolesContext::grantor() {
  return getRuleContext<PrestoSqlParser::GrantorContext>(0);
}

PrestoSqlParser::GrantRolesContext::GrantRolesContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::GrantRolesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterGrantRoles(this);
}
void PrestoSqlParser::GrantRolesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitGrantRoles(this);
}

antlrcpp::Any PrestoSqlParser::GrantRolesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitGrantRoles(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CallContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CallContext::CALL() {
  return getToken(PrestoSqlParser::CALL, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::CallContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

std::vector<PrestoSqlParser::CallArgumentContext*>
PrestoSqlParser::CallContext::callArgument() {
  return getRuleContexts<PrestoSqlParser::CallArgumentContext>();
}

PrestoSqlParser::CallArgumentContext*
PrestoSqlParser::CallContext::callArgument(size_t i) {
  return getRuleContext<PrestoSqlParser::CallArgumentContext>(i);
}

PrestoSqlParser::CallContext::CallContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CallContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCall(this);
}
void PrestoSqlParser::CallContext::exitRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCall(this);
}

antlrcpp::Any PrestoSqlParser::CallContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCall(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RefreshMaterializedViewContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RefreshMaterializedViewContext::REFRESH() {
  return getToken(PrestoSqlParser::REFRESH, 0);
}

tree::TerminalNode*
PrestoSqlParser::RefreshMaterializedViewContext::MATERIALIZED() {
  return getToken(PrestoSqlParser::MATERIALIZED, 0);
}

tree::TerminalNode* PrestoSqlParser::RefreshMaterializedViewContext::VIEW() {
  return getToken(PrestoSqlParser::VIEW, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::RefreshMaterializedViewContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::RefreshMaterializedViewContext::WHERE() {
  return getToken(PrestoSqlParser::WHERE, 0);
}

PrestoSqlParser::BooleanExpressionContext*
PrestoSqlParser::RefreshMaterializedViewContext::booleanExpression() {
  return getRuleContext<PrestoSqlParser::BooleanExpressionContext>(0);
}

PrestoSqlParser::RefreshMaterializedViewContext::RefreshMaterializedViewContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RefreshMaterializedViewContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRefreshMaterializedView(this);
}
void PrestoSqlParser::RefreshMaterializedViewContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRefreshMaterializedView(this);
}

antlrcpp::Any PrestoSqlParser::RefreshMaterializedViewContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRefreshMaterializedView(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowCreateMaterializedViewContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowCreateMaterializedViewContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode*
PrestoSqlParser::ShowCreateMaterializedViewContext::CREATE() {
  return getToken(PrestoSqlParser::CREATE, 0);
}

tree::TerminalNode*
PrestoSqlParser::ShowCreateMaterializedViewContext::MATERIALIZED() {
  return getToken(PrestoSqlParser::MATERIALIZED, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowCreateMaterializedViewContext::VIEW() {
  return getToken(PrestoSqlParser::VIEW, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::ShowCreateMaterializedViewContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::ShowCreateMaterializedViewContext::
    ShowCreateMaterializedViewContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowCreateMaterializedViewContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowCreateMaterializedView(this);
}
void PrestoSqlParser::ShowCreateMaterializedViewContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowCreateMaterializedView(this);
}

antlrcpp::Any PrestoSqlParser::ShowCreateMaterializedViewContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowCreateMaterializedView(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ShowFunctionsContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ShowFunctionsContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowFunctionsContext::FUNCTIONS() {
  return getToken(PrestoSqlParser::FUNCTIONS, 0);
}

tree::TerminalNode* PrestoSqlParser::ShowFunctionsContext::LIKE() {
  return getToken(PrestoSqlParser::LIKE, 0);
}

std::vector<PrestoSqlParser::StringContext*>
PrestoSqlParser::ShowFunctionsContext::string() {
  return getRuleContexts<PrestoSqlParser::StringContext>();
}

PrestoSqlParser::StringContext* PrestoSqlParser::ShowFunctionsContext::string(
    size_t i) {
  return getRuleContext<PrestoSqlParser::StringContext>(i);
}

tree::TerminalNode* PrestoSqlParser::ShowFunctionsContext::ESCAPE() {
  return getToken(PrestoSqlParser::ESCAPE, 0);
}

PrestoSqlParser::ShowFunctionsContext::ShowFunctionsContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ShowFunctionsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterShowFunctions(this);
}
void PrestoSqlParser::ShowFunctionsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitShowFunctions(this);
}

antlrcpp::Any PrestoSqlParser::ShowFunctionsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitShowFunctions(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DescribeOutputContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DescribeOutputContext::DESCRIBE() {
  return getToken(PrestoSqlParser::DESCRIBE, 0);
}

tree::TerminalNode* PrestoSqlParser::DescribeOutputContext::OUTPUT() {
  return getToken(PrestoSqlParser::OUTPUT, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::DescribeOutputContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::DescribeOutputContext::DescribeOutputContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DescribeOutputContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDescribeOutput(this);
}
void PrestoSqlParser::DescribeOutputContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDescribeOutput(this);
}

antlrcpp::Any PrestoSqlParser::DescribeOutputContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDescribeOutput(this);
  else
    return visitor->visitChildren(this);
}
//----------------- GrantContext
//------------------------------------------------------------------

std::vector<tree::TerminalNode*> PrestoSqlParser::GrantContext::GRANT() {
  return getTokens(PrestoSqlParser::GRANT);
}

tree::TerminalNode* PrestoSqlParser::GrantContext::GRANT(size_t i) {
  return getToken(PrestoSqlParser::GRANT, i);
}

tree::TerminalNode* PrestoSqlParser::GrantContext::ON() {
  return getToken(PrestoSqlParser::ON, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::GrantContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::GrantContext::TO() {
  return getToken(PrestoSqlParser::TO, 0);
}

PrestoSqlParser::PrincipalContext* PrestoSqlParser::GrantContext::principal() {
  return getRuleContext<PrestoSqlParser::PrincipalContext>(0);
}

std::vector<PrestoSqlParser::PrivilegeContext*>
PrestoSqlParser::GrantContext::privilege() {
  return getRuleContexts<PrestoSqlParser::PrivilegeContext>();
}

PrestoSqlParser::PrivilegeContext* PrestoSqlParser::GrantContext::privilege(
    size_t i) {
  return getRuleContext<PrestoSqlParser::PrivilegeContext>(i);
}

tree::TerminalNode* PrestoSqlParser::GrantContext::ALL() {
  return getToken(PrestoSqlParser::ALL, 0);
}

tree::TerminalNode* PrestoSqlParser::GrantContext::PRIVILEGES() {
  return getToken(PrestoSqlParser::PRIVILEGES, 0);
}

tree::TerminalNode* PrestoSqlParser::GrantContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::GrantContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

tree::TerminalNode* PrestoSqlParser::GrantContext::OPTION() {
  return getToken(PrestoSqlParser::OPTION, 0);
}

PrestoSqlParser::GrantContext::GrantContext(StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::GrantContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterGrant(this);
}
void PrestoSqlParser::GrantContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitGrant(this);
}

antlrcpp::Any PrestoSqlParser::GrantContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitGrant(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SetTablePropertiesContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::SetTablePropertiesContext::ALTER() {
  return getToken(PrestoSqlParser::ALTER, 0);
}

tree::TerminalNode* PrestoSqlParser::SetTablePropertiesContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::SetTablePropertiesContext::SET() {
  return getToken(PrestoSqlParser::SET, 0);
}

tree::TerminalNode* PrestoSqlParser::SetTablePropertiesContext::PROPERTIES() {
  return getToken(PrestoSqlParser::PROPERTIES, 0);
}

PrestoSqlParser::PropertiesContext*
PrestoSqlParser::SetTablePropertiesContext::properties() {
  return getRuleContext<PrestoSqlParser::PropertiesContext>(0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::SetTablePropertiesContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::SetTablePropertiesContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::SetTablePropertiesContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

PrestoSqlParser::SetTablePropertiesContext::SetTablePropertiesContext(
    StatementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SetTablePropertiesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSetTableProperties(this);
}
void PrestoSqlParser::SetTablePropertiesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSetTableProperties(this);
}

antlrcpp::Any PrestoSqlParser::SetTablePropertiesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSetTableProperties(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::StatementContext* PrestoSqlParser::statement() {
  StatementContext* _localctx =
      _tracker.createInstance<StatementContext>(_ctx, getState());
  enterRule(_localctx, 6, PrestoSqlParser::RuleStatement);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(924);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 102, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::StatementDefaultContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(187);
        query();
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UseContext>(_localctx);
        enterOuterAlt(_localctx, 2);
        setState(188);
        match(PrestoSqlParser::USE);
        setState(189);
        antlrcpp::downCast<UseContext*>(_localctx)->schema = identifier();
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UseContext>(_localctx);
        enterOuterAlt(_localctx, 3);
        setState(190);
        match(PrestoSqlParser::USE);
        setState(191);
        antlrcpp::downCast<UseContext*>(_localctx)->catalog = identifier();
        setState(192);
        match(PrestoSqlParser::T__0);
        setState(193);
        antlrcpp::downCast<UseContext*>(_localctx)->schema = identifier();
        break;
      }

      case 4: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CreateSchemaContext>(
                _localctx);
        enterOuterAlt(_localctx, 4);
        setState(195);
        match(PrestoSqlParser::CREATE);
        setState(196);
        match(PrestoSqlParser::SCHEMA);
        setState(200);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 0, _ctx)) {
          case 1: {
            setState(197);
            match(PrestoSqlParser::IF);
            setState(198);
            match(PrestoSqlParser::NOT);
            setState(199);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(202);
        qualifiedName();
        setState(205);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(203);
          match(PrestoSqlParser::WITH);
          setState(204);
          properties();
        }
        break;
      }

      case 5: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DropSchemaContext>(
            _localctx);
        enterOuterAlt(_localctx, 5);
        setState(207);
        match(PrestoSqlParser::DROP);
        setState(208);
        match(PrestoSqlParser::SCHEMA);
        setState(211);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 2, _ctx)) {
          case 1: {
            setState(209);
            match(PrestoSqlParser::IF);
            setState(210);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(213);
        qualifiedName();
        setState(215);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::CASCADE ||
            _la == PrestoSqlParser::RESTRICT) {
          setState(214);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::CASCADE ||
                _la == PrestoSqlParser::RESTRICT)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        break;
      }

      case 6: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RenameSchemaContext>(
                _localctx);
        enterOuterAlt(_localctx, 6);
        setState(217);
        match(PrestoSqlParser::ALTER);
        setState(218);
        match(PrestoSqlParser::SCHEMA);
        setState(219);
        qualifiedName();
        setState(220);
        match(PrestoSqlParser::RENAME);
        setState(221);
        match(PrestoSqlParser::TO);
        setState(222);
        identifier();
        break;
      }

      case 7: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::CreateTableAsSelectContext>(
                    _localctx);
        enterOuterAlt(_localctx, 7);
        setState(224);
        match(PrestoSqlParser::CREATE);
        setState(225);
        match(PrestoSqlParser::TABLE);
        setState(229);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 4, _ctx)) {
          case 1: {
            setState(226);
            match(PrestoSqlParser::IF);
            setState(227);
            match(PrestoSqlParser::NOT);
            setState(228);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(231);
        qualifiedName();
        setState(233);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::T__1) {
          setState(232);
          columnAliases();
        }
        setState(237);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::COMMENT) {
          setState(235);
          match(PrestoSqlParser::COMMENT);
          setState(236);
          string();
        }
        setState(241);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(239);
          match(PrestoSqlParser::WITH);
          setState(240);
          properties();
        }
        setState(243);
        match(PrestoSqlParser::AS);
        setState(249);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 8, _ctx)) {
          case 1: {
            setState(244);
            query();
            break;
          }

          case 2: {
            setState(245);
            match(PrestoSqlParser::T__1);
            setState(246);
            query();
            setState(247);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        setState(256);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(251);
          match(PrestoSqlParser::WITH);
          setState(253);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::NO) {
            setState(252);
            match(PrestoSqlParser::NO);
          }
          setState(255);
          match(PrestoSqlParser::DATA);
        }
        break;
      }

      case 8: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CreateTableContext>(
                _localctx);
        enterOuterAlt(_localctx, 8);
        setState(258);
        match(PrestoSqlParser::CREATE);
        setState(259);
        match(PrestoSqlParser::TABLE);
        setState(263);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 11, _ctx)) {
          case 1: {
            setState(260);
            match(PrestoSqlParser::IF);
            setState(261);
            match(PrestoSqlParser::NOT);
            setState(262);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(265);
        qualifiedName();
        setState(266);
        match(PrestoSqlParser::T__1);
        setState(267);
        tableElement();
        setState(272);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(268);
          match(PrestoSqlParser::T__3);
          setState(269);
          tableElement();
          setState(274);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(275);
        match(PrestoSqlParser::T__2);
        setState(278);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::COMMENT) {
          setState(276);
          match(PrestoSqlParser::COMMENT);
          setState(277);
          string();
        }
        setState(282);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(280);
          match(PrestoSqlParser::WITH);
          setState(281);
          properties();
        }
        break;
      }

      case 9: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DropTableContext>(
            _localctx);
        enterOuterAlt(_localctx, 9);
        setState(284);
        match(PrestoSqlParser::DROP);
        setState(285);
        match(PrestoSqlParser::TABLE);
        setState(288);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 15, _ctx)) {
          case 1: {
            setState(286);
            match(PrestoSqlParser::IF);
            setState(287);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(290);
        qualifiedName();
        break;
      }

      case 10: {
        _localctx = _tracker.createInstance<PrestoSqlParser::InsertIntoContext>(
            _localctx);
        enterOuterAlt(_localctx, 10);
        setState(291);
        match(PrestoSqlParser::INSERT);
        setState(292);
        match(PrestoSqlParser::INTO);
        setState(293);
        qualifiedName();
        setState(295);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 16, _ctx)) {
          case 1: {
            setState(294);
            columnAliases();
            break;
          }

          default:
            break;
        }
        setState(297);
        query();
        break;
      }

      case 11: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DeleteContext>(_localctx);
        enterOuterAlt(_localctx, 11);
        setState(299);
        match(PrestoSqlParser::DELETE);
        setState(300);
        match(PrestoSqlParser::FROM);
        setState(301);
        qualifiedName();
        setState(304);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WHERE) {
          setState(302);
          match(PrestoSqlParser::WHERE);
          setState(303);
          booleanExpression(0);
        }
        break;
      }

      case 12: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TruncateTableContext>(
                _localctx);
        enterOuterAlt(_localctx, 12);
        setState(306);
        match(PrestoSqlParser::TRUNCATE);
        setState(307);
        match(PrestoSqlParser::TABLE);
        setState(308);
        qualifiedName();
        break;
      }

      case 13: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RenameTableContext>(
                _localctx);
        enterOuterAlt(_localctx, 13);
        setState(309);
        match(PrestoSqlParser::ALTER);
        setState(310);
        match(PrestoSqlParser::TABLE);
        setState(313);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 18, _ctx)) {
          case 1: {
            setState(311);
            match(PrestoSqlParser::IF);
            setState(312);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(315);
        antlrcpp::downCast<RenameTableContext*>(_localctx)->from =
            qualifiedName();
        setState(316);
        match(PrestoSqlParser::RENAME);
        setState(317);
        match(PrestoSqlParser::TO);
        setState(318);
        antlrcpp::downCast<RenameTableContext*>(_localctx)->to =
            qualifiedName();
        break;
      }

      case 14: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RenameColumnContext>(
                _localctx);
        enterOuterAlt(_localctx, 14);
        setState(320);
        match(PrestoSqlParser::ALTER);
        setState(321);
        match(PrestoSqlParser::TABLE);
        setState(324);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 19, _ctx)) {
          case 1: {
            setState(322);
            match(PrestoSqlParser::IF);
            setState(323);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(326);
        antlrcpp::downCast<RenameColumnContext*>(_localctx)->tableName =
            qualifiedName();
        setState(327);
        match(PrestoSqlParser::RENAME);
        setState(328);
        match(PrestoSqlParser::COLUMN);
        setState(331);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 20, _ctx)) {
          case 1: {
            setState(329);
            match(PrestoSqlParser::IF);
            setState(330);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(333);
        antlrcpp::downCast<RenameColumnContext*>(_localctx)->from =
            identifier();
        setState(334);
        match(PrestoSqlParser::TO);
        setState(335);
        antlrcpp::downCast<RenameColumnContext*>(_localctx)->to = identifier();
        break;
      }

      case 15: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DropColumnContext>(
            _localctx);
        enterOuterAlt(_localctx, 15);
        setState(337);
        match(PrestoSqlParser::ALTER);
        setState(338);
        match(PrestoSqlParser::TABLE);
        setState(341);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 21, _ctx)) {
          case 1: {
            setState(339);
            match(PrestoSqlParser::IF);
            setState(340);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(343);
        antlrcpp::downCast<DropColumnContext*>(_localctx)->tableName =
            qualifiedName();
        setState(344);
        match(PrestoSqlParser::DROP);
        setState(345);
        match(PrestoSqlParser::COLUMN);
        setState(348);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 22, _ctx)) {
          case 1: {
            setState(346);
            match(PrestoSqlParser::IF);
            setState(347);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(350);
        antlrcpp::downCast<DropColumnContext*>(_localctx)->column =
            qualifiedName();
        break;
      }

      case 16: {
        _localctx = _tracker.createInstance<PrestoSqlParser::AddColumnContext>(
            _localctx);
        enterOuterAlt(_localctx, 16);
        setState(352);
        match(PrestoSqlParser::ALTER);
        setState(353);
        match(PrestoSqlParser::TABLE);
        setState(356);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 23, _ctx)) {
          case 1: {
            setState(354);
            match(PrestoSqlParser::IF);
            setState(355);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(358);
        antlrcpp::downCast<AddColumnContext*>(_localctx)->tableName =
            qualifiedName();
        setState(359);
        match(PrestoSqlParser::ADD);
        setState(360);
        match(PrestoSqlParser::COLUMN);
        setState(364);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 24, _ctx)) {
          case 1: {
            setState(361);
            match(PrestoSqlParser::IF);
            setState(362);
            match(PrestoSqlParser::NOT);
            setState(363);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(366);
        antlrcpp::downCast<AddColumnContext*>(_localctx)->column =
            columnDefinition();
        break;
      }

      case 17: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::AddConstraintContext>(
                _localctx);
        enterOuterAlt(_localctx, 17);
        setState(368);
        match(PrestoSqlParser::ALTER);
        setState(369);
        match(PrestoSqlParser::TABLE);
        setState(372);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 25, _ctx)) {
          case 1: {
            setState(370);
            match(PrestoSqlParser::IF);
            setState(371);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(374);
        antlrcpp::downCast<AddConstraintContext*>(_localctx)->tableName =
            qualifiedName();
        setState(375);
        match(PrestoSqlParser::ADD);
        setState(376);
        constraintSpecification();
        break;
      }

      case 18: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DropConstraintContext>(
                _localctx);
        enterOuterAlt(_localctx, 18);
        setState(378);
        match(PrestoSqlParser::ALTER);
        setState(379);
        match(PrestoSqlParser::TABLE);
        setState(382);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 26, _ctx)) {
          case 1: {
            setState(380);
            match(PrestoSqlParser::IF);
            setState(381);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(384);
        antlrcpp::downCast<DropConstraintContext*>(_localctx)->tableName =
            qualifiedName();
        setState(385);
        match(PrestoSqlParser::DROP);
        setState(386);
        match(PrestoSqlParser::CONSTRAINT);
        setState(389);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 27, _ctx)) {
          case 1: {
            setState(387);
            match(PrestoSqlParser::IF);
            setState(388);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(391);
        antlrcpp::downCast<DropConstraintContext*>(_localctx)->name =
            identifier();
        break;
      }

      case 19: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::AlterColumnSetNotNullContext>(
                    _localctx);
        enterOuterAlt(_localctx, 19);
        setState(393);
        match(PrestoSqlParser::ALTER);
        setState(394);
        match(PrestoSqlParser::TABLE);
        setState(397);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 28, _ctx)) {
          case 1: {
            setState(395);
            match(PrestoSqlParser::IF);
            setState(396);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(399);
        antlrcpp::downCast<AlterColumnSetNotNullContext*>(_localctx)
            ->tableName = qualifiedName();
        setState(400);
        match(PrestoSqlParser::ALTER);
        setState(402);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 29, _ctx)) {
          case 1: {
            setState(401);
            match(PrestoSqlParser::COLUMN);
            break;
          }

          default:
            break;
        }
        setState(404);
        antlrcpp::downCast<AlterColumnSetNotNullContext*>(_localctx)->column =
            identifier();
        setState(405);
        match(PrestoSqlParser::SET);
        setState(406);
        match(PrestoSqlParser::NOT);
        setState(407);
        match(PrestoSqlParser::NULL_LITERAL);
        break;
      }

      case 20: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::AlterColumnDropNotNullContext>(
                    _localctx);
        enterOuterAlt(_localctx, 20);
        setState(409);
        match(PrestoSqlParser::ALTER);
        setState(410);
        match(PrestoSqlParser::TABLE);
        setState(413);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 30, _ctx)) {
          case 1: {
            setState(411);
            match(PrestoSqlParser::IF);
            setState(412);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(415);
        antlrcpp::downCast<AlterColumnDropNotNullContext*>(_localctx)
            ->tableName = qualifiedName();
        setState(416);
        match(PrestoSqlParser::ALTER);
        setState(418);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 31, _ctx)) {
          case 1: {
            setState(417);
            match(PrestoSqlParser::COLUMN);
            break;
          }

          default:
            break;
        }
        setState(420);
        antlrcpp::downCast<AlterColumnDropNotNullContext*>(_localctx)->column =
            identifier();
        setState(421);
        match(PrestoSqlParser::DROP);
        setState(422);
        match(PrestoSqlParser::NOT);
        setState(423);
        match(PrestoSqlParser::NULL_LITERAL);
        break;
      }

      case 21: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SetTablePropertiesContext>(
                _localctx);
        enterOuterAlt(_localctx, 21);
        setState(425);
        match(PrestoSqlParser::ALTER);
        setState(426);
        match(PrestoSqlParser::TABLE);
        setState(429);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 32, _ctx)) {
          case 1: {
            setState(427);
            match(PrestoSqlParser::IF);
            setState(428);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(431);
        antlrcpp::downCast<SetTablePropertiesContext*>(_localctx)->tableName =
            qualifiedName();
        setState(432);
        match(PrestoSqlParser::SET);
        setState(433);
        match(PrestoSqlParser::PROPERTIES);
        setState(434);
        properties();
        break;
      }

      case 22: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::AnalyzeContext>(_localctx);
        enterOuterAlt(_localctx, 22);
        setState(436);
        match(PrestoSqlParser::ANALYZE);
        setState(437);
        qualifiedName();
        setState(440);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(438);
          match(PrestoSqlParser::WITH);
          setState(439);
          properties();
        }
        break;
      }

      case 23: {
        _localctx = _tracker.createInstance<PrestoSqlParser::CreateTypeContext>(
            _localctx);
        enterOuterAlt(_localctx, 23);
        setState(442);
        match(PrestoSqlParser::CREATE);
        setState(443);
        match(PrestoSqlParser::TYPE);
        setState(444);
        qualifiedName();
        setState(445);
        match(PrestoSqlParser::AS);
        setState(458);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case PrestoSqlParser::T__1: {
            setState(446);
            match(PrestoSqlParser::T__1);
            setState(447);
            sqlParameterDeclaration();
            setState(452);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == PrestoSqlParser::T__3) {
              setState(448);
              match(PrestoSqlParser::T__3);
              setState(449);
              sqlParameterDeclaration();
              setState(454);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
            setState(455);
            match(PrestoSqlParser::T__2);
            break;
          }

          case PrestoSqlParser::ADD:
          case PrestoSqlParser::ADMIN:
          case PrestoSqlParser::ALL:
          case PrestoSqlParser::ANALYZE:
          case PrestoSqlParser::ANY:
          case PrestoSqlParser::ARRAY:
          case PrestoSqlParser::ASC:
          case PrestoSqlParser::AT:
          case PrestoSqlParser::BEFORE:
          case PrestoSqlParser::BERNOULLI:
          case PrestoSqlParser::CALL:
          case PrestoSqlParser::CALLED:
          case PrestoSqlParser::CASCADE:
          case PrestoSqlParser::CATALOGS:
          case PrestoSqlParser::COLUMN:
          case PrestoSqlParser::COLUMNS:
          case PrestoSqlParser::COMMENT:
          case PrestoSqlParser::COMMIT:
          case PrestoSqlParser::COMMITTED:
          case PrestoSqlParser::CURRENT:
          case PrestoSqlParser::CURRENT_ROLE:
          case PrestoSqlParser::DATA:
          case PrestoSqlParser::DATE:
          case PrestoSqlParser::DAY:
          case PrestoSqlParser::DEFINER:
          case PrestoSqlParser::DESC:
          case PrestoSqlParser::DETERMINISTIC:
          case PrestoSqlParser::DISABLED:
          case PrestoSqlParser::DISTRIBUTED:
          case PrestoSqlParser::ENABLED:
          case PrestoSqlParser::ENFORCED:
          case PrestoSqlParser::EXCLUDING:
          case PrestoSqlParser::EXPLAIN:
          case PrestoSqlParser::EXTERNAL:
          case PrestoSqlParser::FETCH:
          case PrestoSqlParser::FILTER:
          case PrestoSqlParser::FIRST:
          case PrestoSqlParser::FOLLOWING:
          case PrestoSqlParser::FORMAT:
          case PrestoSqlParser::FUNCTION:
          case PrestoSqlParser::FUNCTIONS:
          case PrestoSqlParser::GRANT:
          case PrestoSqlParser::GRANTED:
          case PrestoSqlParser::GRANTS:
          case PrestoSqlParser::GRAPHVIZ:
          case PrestoSqlParser::GROUPS:
          case PrestoSqlParser::HOUR:
          case PrestoSqlParser::IF:
          case PrestoSqlParser::IGNORE:
          case PrestoSqlParser::INCLUDING:
          case PrestoSqlParser::INPUT:
          case PrestoSqlParser::INTERVAL:
          case PrestoSqlParser::INVOKER:
          case PrestoSqlParser::IO:
          case PrestoSqlParser::ISOLATION:
          case PrestoSqlParser::JSON:
          case PrestoSqlParser::KEY:
          case PrestoSqlParser::LANGUAGE:
          case PrestoSqlParser::LAST:
          case PrestoSqlParser::LATERAL:
          case PrestoSqlParser::LEVEL:
          case PrestoSqlParser::LIMIT:
          case PrestoSqlParser::LOGICAL:
          case PrestoSqlParser::MAP:
          case PrestoSqlParser::MATERIALIZED:
          case PrestoSqlParser::MINUTE:
          case PrestoSqlParser::MONTH:
          case PrestoSqlParser::NAME:
          case PrestoSqlParser::NFC:
          case PrestoSqlParser::NFD:
          case PrestoSqlParser::NFKC:
          case PrestoSqlParser::NFKD:
          case PrestoSqlParser::NO:
          case PrestoSqlParser::NONE:
          case PrestoSqlParser::NULLIF:
          case PrestoSqlParser::NULLS:
          case PrestoSqlParser::OF:
          case PrestoSqlParser::OFFSET:
          case PrestoSqlParser::ONLY:
          case PrestoSqlParser::OPTION:
          case PrestoSqlParser::ORDINALITY:
          case PrestoSqlParser::OUTPUT:
          case PrestoSqlParser::OVER:
          case PrestoSqlParser::PARTITION:
          case PrestoSqlParser::PARTITIONS:
          case PrestoSqlParser::POSITION:
          case PrestoSqlParser::PRECEDING:
          case PrestoSqlParser::PRIMARY:
          case PrestoSqlParser::PRIVILEGES:
          case PrestoSqlParser::PROPERTIES:
          case PrestoSqlParser::RANGE:
          case PrestoSqlParser::READ:
          case PrestoSqlParser::REFRESH:
          case PrestoSqlParser::RELY:
          case PrestoSqlParser::RENAME:
          case PrestoSqlParser::REPEATABLE:
          case PrestoSqlParser::REPLACE:
          case PrestoSqlParser::RESET:
          case PrestoSqlParser::RESPECT:
          case PrestoSqlParser::RESTRICT:
          case PrestoSqlParser::RETURN:
          case PrestoSqlParser::RETURNS:
          case PrestoSqlParser::REVOKE:
          case PrestoSqlParser::ROLE:
          case PrestoSqlParser::ROLES:
          case PrestoSqlParser::ROLLBACK:
          case PrestoSqlParser::ROW:
          case PrestoSqlParser::ROWS:
          case PrestoSqlParser::SCHEMA:
          case PrestoSqlParser::SCHEMAS:
          case PrestoSqlParser::SECOND:
          case PrestoSqlParser::SECURITY:
          case PrestoSqlParser::SERIALIZABLE:
          case PrestoSqlParser::SESSION:
          case PrestoSqlParser::SET:
          case PrestoSqlParser::SETS:
          case PrestoSqlParser::SHOW:
          case PrestoSqlParser::SOME:
          case PrestoSqlParser::SQL:
          case PrestoSqlParser::START:
          case PrestoSqlParser::STATS:
          case PrestoSqlParser::SUBSTRING:
          case PrestoSqlParser::SYSTEM:
          case PrestoSqlParser::SYSTEM_TIME:
          case PrestoSqlParser::SYSTEM_VERSION:
          case PrestoSqlParser::TABLES:
          case PrestoSqlParser::TABLESAMPLE:
          case PrestoSqlParser::TEMPORARY:
          case PrestoSqlParser::TEXT:
          case PrestoSqlParser::TIME:
          case PrestoSqlParser::TIMESTAMP:
          case PrestoSqlParser::TO:
          case PrestoSqlParser::TRANSACTION:
          case PrestoSqlParser::TRUNCATE:
          case PrestoSqlParser::TRY_CAST:
          case PrestoSqlParser::TYPE:
          case PrestoSqlParser::UNBOUNDED:
          case PrestoSqlParser::UNCOMMITTED:
          case PrestoSqlParser::UNIQUE:
          case PrestoSqlParser::UPDATE:
          case PrestoSqlParser::USE:
          case PrestoSqlParser::USER:
          case PrestoSqlParser::VALIDATE:
          case PrestoSqlParser::VERBOSE:
          case PrestoSqlParser::VERSION:
          case PrestoSqlParser::VIEW:
          case PrestoSqlParser::WORK:
          case PrestoSqlParser::WRITE:
          case PrestoSqlParser::YEAR:
          case PrestoSqlParser::ZONE:
          case PrestoSqlParser::IDENTIFIER:
          case PrestoSqlParser::DIGIT_IDENTIFIER:
          case PrestoSqlParser::QUOTED_IDENTIFIER:
          case PrestoSqlParser::BACKQUOTED_IDENTIFIER:
          case PrestoSqlParser::TIME_WITH_TIME_ZONE:
          case PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE:
          case PrestoSqlParser::DOUBLE_PRECISION: {
            setState(457);
            type(0);
            break;
          }

          default:
            throw NoViableAltException(this);
        }
        break;
      }

      case 24: {
        _localctx = _tracker.createInstance<PrestoSqlParser::CreateViewContext>(
            _localctx);
        enterOuterAlt(_localctx, 24);
        setState(460);
        match(PrestoSqlParser::CREATE);
        setState(463);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::OR) {
          setState(461);
          match(PrestoSqlParser::OR);
          setState(462);
          match(PrestoSqlParser::REPLACE);
        }
        setState(465);
        match(PrestoSqlParser::VIEW);
        setState(466);
        qualifiedName();
        setState(469);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::SECURITY) {
          setState(467);
          match(PrestoSqlParser::SECURITY);
          setState(468);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::DEFINER

                || _la == PrestoSqlParser::INVOKER)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(471);
        match(PrestoSqlParser::AS);
        setState(472);
        query();
        break;
      }

      case 25: {
        _localctx = _tracker.createInstance<PrestoSqlParser::RenameViewContext>(
            _localctx);
        enterOuterAlt(_localctx, 25);
        setState(474);
        match(PrestoSqlParser::ALTER);
        setState(475);
        match(PrestoSqlParser::VIEW);
        setState(478);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 38, _ctx)) {
          case 1: {
            setState(476);
            match(PrestoSqlParser::IF);
            setState(477);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(480);
        antlrcpp::downCast<RenameViewContext*>(_localctx)->from =
            qualifiedName();
        setState(481);
        match(PrestoSqlParser::RENAME);
        setState(482);
        match(PrestoSqlParser::TO);
        setState(483);
        antlrcpp::downCast<RenameViewContext*>(_localctx)->to = qualifiedName();
        break;
      }

      case 26: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DropViewContext>(
            _localctx);
        enterOuterAlt(_localctx, 26);
        setState(485);
        match(PrestoSqlParser::DROP);
        setState(486);
        match(PrestoSqlParser::VIEW);
        setState(489);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 39, _ctx)) {
          case 1: {
            setState(487);
            match(PrestoSqlParser::IF);
            setState(488);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(491);
        qualifiedName();
        break;
      }

      case 27: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::CreateMaterializedViewContext>(
                    _localctx);
        enterOuterAlt(_localctx, 27);
        setState(492);
        match(PrestoSqlParser::CREATE);
        setState(493);
        match(PrestoSqlParser::MATERIALIZED);
        setState(494);
        match(PrestoSqlParser::VIEW);
        setState(498);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 40, _ctx)) {
          case 1: {
            setState(495);
            match(PrestoSqlParser::IF);
            setState(496);
            match(PrestoSqlParser::NOT);
            setState(497);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(500);
        qualifiedName();
        setState(503);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::COMMENT) {
          setState(501);
          match(PrestoSqlParser::COMMENT);
          setState(502);
          string();
        }
        setState(507);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(505);
          match(PrestoSqlParser::WITH);
          setState(506);
          properties();
        }
        setState(509);
        match(PrestoSqlParser::AS);
        setState(515);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 43, _ctx)) {
          case 1: {
            setState(510);
            query();
            break;
          }

          case 2: {
            setState(511);
            match(PrestoSqlParser::T__1);
            setState(512);
            query();
            setState(513);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 28: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::DropMaterializedViewContext>(
                    _localctx);
        enterOuterAlt(_localctx, 28);
        setState(517);
        match(PrestoSqlParser::DROP);
        setState(518);
        match(PrestoSqlParser::MATERIALIZED);
        setState(519);
        match(PrestoSqlParser::VIEW);
        setState(522);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 44, _ctx)) {
          case 1: {
            setState(520);
            match(PrestoSqlParser::IF);
            setState(521);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(524);
        qualifiedName();
        break;
      }

      case 29: {
        _localctx = _tracker.createInstance<
            PrestoSqlParser::RefreshMaterializedViewContext>(_localctx);
        enterOuterAlt(_localctx, 29);
        setState(525);
        match(PrestoSqlParser::REFRESH);
        setState(526);
        match(PrestoSqlParser::MATERIALIZED);
        setState(527);
        match(PrestoSqlParser::VIEW);
        setState(528);
        qualifiedName();
        setState(529);
        match(PrestoSqlParser::WHERE);
        setState(530);
        booleanExpression(0);
        break;
      }

      case 30: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CreateFunctionContext>(
                _localctx);
        enterOuterAlt(_localctx, 30);
        setState(532);
        match(PrestoSqlParser::CREATE);
        setState(535);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::OR) {
          setState(533);
          match(PrestoSqlParser::OR);
          setState(534);
          match(PrestoSqlParser::REPLACE);
        }
        setState(538);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::TEMPORARY) {
          setState(537);
          match(PrestoSqlParser::TEMPORARY);
        }
        setState(540);
        match(PrestoSqlParser::FUNCTION);
        setState(541);
        antlrcpp::downCast<CreateFunctionContext*>(_localctx)->functionName =
            qualifiedName();
        setState(542);
        match(PrestoSqlParser::T__1);
        setState(551);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) &
              ((1ULL << PrestoSqlParser::ADD) |
               (1ULL << PrestoSqlParser::ADMIN) |
               (1ULL << PrestoSqlParser::ALL) |
               (1ULL << PrestoSqlParser::ANALYZE) |
               (1ULL << PrestoSqlParser::ANY) |
               (1ULL << PrestoSqlParser::ARRAY) |
               (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
               (1ULL << PrestoSqlParser::BEFORE) |
               (1ULL << PrestoSqlParser::BERNOULLI) |
               (1ULL << PrestoSqlParser::CALL) |
               (1ULL << PrestoSqlParser::CALLED) |
               (1ULL << PrestoSqlParser::CASCADE) |
               (1ULL << PrestoSqlParser::CATALOGS) |
               (1ULL << PrestoSqlParser::COLUMN) |
               (1ULL << PrestoSqlParser::COLUMNS) |
               (1ULL << PrestoSqlParser::COMMENT) |
               (1ULL << PrestoSqlParser::COMMIT) |
               (1ULL << PrestoSqlParser::COMMITTED) |
               (1ULL << PrestoSqlParser::CURRENT) |
               (1ULL << PrestoSqlParser::CURRENT_ROLE) |
               (1ULL << PrestoSqlParser::DATA) |
               (1ULL << PrestoSqlParser::DATE) |
               (1ULL << PrestoSqlParser::DAY) |
               (1ULL << PrestoSqlParser::DEFINER) |
               (1ULL << PrestoSqlParser::DESC) |
               (1ULL << PrestoSqlParser::DETERMINISTIC) |
               (1ULL << PrestoSqlParser::DISABLED) |
               (1ULL << PrestoSqlParser::DISTRIBUTED) |
               (1ULL << PrestoSqlParser::ENABLED) |
               (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) &
              ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
               (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
               (1ULL << (PrestoSqlParser::FETCH - 66)) |
               (1ULL << (PrestoSqlParser::FILTER - 66)) |
               (1ULL << (PrestoSqlParser::FIRST - 66)) |
               (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
               (1ULL << (PrestoSqlParser::FORMAT - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
               (1ULL << (PrestoSqlParser::GRANT - 66)) |
               (1ULL << (PrestoSqlParser::GRANTED - 66)) |
               (1ULL << (PrestoSqlParser::GRANTS - 66)) |
               (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
               (1ULL << (PrestoSqlParser::GROUPS - 66)) |
               (1ULL << (PrestoSqlParser::HOUR - 66)) |
               (1ULL << (PrestoSqlParser::IF - 66)) |
               (1ULL << (PrestoSqlParser::IGNORE - 66)) |
               (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::INPUT - 66)) |
               (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
               (1ULL << (PrestoSqlParser::INVOKER - 66)) |
               (1ULL << (PrestoSqlParser::IO - 66)) |
               (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
               (1ULL << (PrestoSqlParser::JSON - 66)) |
               (1ULL << (PrestoSqlParser::KEY - 66)) |
               (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
               (1ULL << (PrestoSqlParser::LAST - 66)) |
               (1ULL << (PrestoSqlParser::LATERAL - 66)) |
               (1ULL << (PrestoSqlParser::LEVEL - 66)) |
               (1ULL << (PrestoSqlParser::LIMIT - 66)) |
               (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
               (1ULL << (PrestoSqlParser::MAP - 66)) |
               (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
               (1ULL << (PrestoSqlParser::MINUTE - 66)) |
               (1ULL << (PrestoSqlParser::MONTH - 66)) |
               (1ULL << (PrestoSqlParser::NAME - 66)) |
               (1ULL << (PrestoSqlParser::NFC - 66)) |
               (1ULL << (PrestoSqlParser::NFD - 66)) |
               (1ULL << (PrestoSqlParser::NFKC - 66)) |
               (1ULL << (PrestoSqlParser::NFKD - 66)) |
               (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) &
              ((1ULL << (PrestoSqlParser::NONE - 130)) |
               (1ULL << (PrestoSqlParser::NULLIF - 130)) |
               (1ULL << (PrestoSqlParser::NULLS - 130)) |
               (1ULL << (PrestoSqlParser::OF - 130)) |
               (1ULL << (PrestoSqlParser::OFFSET - 130)) |
               (1ULL << (PrestoSqlParser::ONLY - 130)) |
               (1ULL << (PrestoSqlParser::OPTION - 130)) |
               (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
               (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
               (1ULL << (PrestoSqlParser::OVER - 130)) |
               (1ULL << (PrestoSqlParser::PARTITION - 130)) |
               (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
               (1ULL << (PrestoSqlParser::POSITION - 130)) |
               (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
               (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
               (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
               (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
               (1ULL << (PrestoSqlParser::RANGE - 130)) |
               (1ULL << (PrestoSqlParser::READ - 130)) |
               (1ULL << (PrestoSqlParser::REFRESH - 130)) |
               (1ULL << (PrestoSqlParser::RELY - 130)) |
               (1ULL << (PrestoSqlParser::RENAME - 130)) |
               (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
               (1ULL << (PrestoSqlParser::REPLACE - 130)) |
               (1ULL << (PrestoSqlParser::RESET - 130)) |
               (1ULL << (PrestoSqlParser::RESPECT - 130)) |
               (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
               (1ULL << (PrestoSqlParser::RETURN - 130)) |
               (1ULL << (PrestoSqlParser::RETURNS - 130)) |
               (1ULL << (PrestoSqlParser::REVOKE - 130)) |
               (1ULL << (PrestoSqlParser::ROLE - 130)) |
               (1ULL << (PrestoSqlParser::ROLES - 130)) |
               (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
               (1ULL << (PrestoSqlParser::ROW - 130)) |
               (1ULL << (PrestoSqlParser::ROWS - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
               (1ULL << (PrestoSqlParser::SECOND - 130)) |
               (1ULL << (PrestoSqlParser::SECURITY - 130)) |
               (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
               (1ULL << (PrestoSqlParser::SESSION - 130)) |
               (1ULL << (PrestoSqlParser::SET - 130)) |
               (1ULL << (PrestoSqlParser::SETS - 130)) |
               (1ULL << (PrestoSqlParser::SHOW - 130)) |
               (1ULL << (PrestoSqlParser::SOME - 130)) |
               (1ULL << (PrestoSqlParser::SQL - 130)) |
               (1ULL << (PrestoSqlParser::START - 130)) |
               (1ULL << (PrestoSqlParser::STATS - 130)) |
               (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
            ((((_la - 195) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 195)) &
              ((1ULL << (PrestoSqlParser::TABLES - 195)) |
               (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
               (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
               (1ULL << (PrestoSqlParser::TEXT - 195)) |
               (1ULL << (PrestoSqlParser::TIME - 195)) |
               (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
               (1ULL << (PrestoSqlParser::TO - 195)) |
               (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
               (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
               (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
               (1ULL << (PrestoSqlParser::TYPE - 195)) |
               (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
               (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
               (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
               (1ULL << (PrestoSqlParser::UPDATE - 195)) |
               (1ULL << (PrestoSqlParser::USE - 195)) |
               (1ULL << (PrestoSqlParser::USER - 195)) |
               (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
               (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
               (1ULL << (PrestoSqlParser::VERSION - 195)) |
               (1ULL << (PrestoSqlParser::VIEW - 195)) |
               (1ULL << (PrestoSqlParser::WORK - 195)) |
               (1ULL << (PrestoSqlParser::WRITE - 195)) |
               (1ULL << (PrestoSqlParser::YEAR - 195)) |
               (1ULL << (PrestoSqlParser::ZONE - 195)) |
               (1ULL << (PrestoSqlParser::IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DIGIT_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::QUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::BACKQUOTED_IDENTIFIER - 195)))) !=
                 0)) {
          setState(543);
          sqlParameterDeclaration();
          setState(548);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(544);
            match(PrestoSqlParser::T__3);
            setState(545);
            sqlParameterDeclaration();
            setState(550);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(553);
        match(PrestoSqlParser::T__2);
        setState(554);
        match(PrestoSqlParser::RETURNS);
        setState(555);
        antlrcpp::downCast<CreateFunctionContext*>(_localctx)->returnType =
            type(0);
        setState(558);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::COMMENT) {
          setState(556);
          match(PrestoSqlParser::COMMENT);
          setState(557);
          string();
        }
        setState(560);
        routineCharacteristics();
        setState(561);
        routineBody();
        break;
      }

      case 31: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::AlterFunctionContext>(
                _localctx);
        enterOuterAlt(_localctx, 31);
        setState(563);
        match(PrestoSqlParser::ALTER);
        setState(564);
        match(PrestoSqlParser::FUNCTION);
        setState(565);
        qualifiedName();
        setState(567);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::T__1) {
          setState(566);
          types();
        }
        setState(569);
        alterRoutineCharacteristics();
        break;
      }

      case 32: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DropFunctionContext>(
                _localctx);
        enterOuterAlt(_localctx, 32);
        setState(571);
        match(PrestoSqlParser::DROP);
        setState(573);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::TEMPORARY) {
          setState(572);
          match(PrestoSqlParser::TEMPORARY);
        }
        setState(575);
        match(PrestoSqlParser::FUNCTION);
        setState(578);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 52, _ctx)) {
          case 1: {
            setState(576);
            match(PrestoSqlParser::IF);
            setState(577);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(580);
        qualifiedName();
        setState(582);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::T__1) {
          setState(581);
          types();
        }
        break;
      }

      case 33: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CallContext>(_localctx);
        enterOuterAlt(_localctx, 33);
        setState(584);
        match(PrestoSqlParser::CALL);
        setState(585);
        qualifiedName();
        setState(586);
        match(PrestoSqlParser::T__1);
        setState(595);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) &
              ((1ULL << PrestoSqlParser::T__1) |
               (1ULL << PrestoSqlParser::T__4) |
               (1ULL << PrestoSqlParser::ADD) |
               (1ULL << PrestoSqlParser::ADMIN) |
               (1ULL << PrestoSqlParser::ALL) |
               (1ULL << PrestoSqlParser::ANALYZE) |
               (1ULL << PrestoSqlParser::ANY) |
               (1ULL << PrestoSqlParser::ARRAY) |
               (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
               (1ULL << PrestoSqlParser::BEFORE) |
               (1ULL << PrestoSqlParser::BERNOULLI) |
               (1ULL << PrestoSqlParser::CALL) |
               (1ULL << PrestoSqlParser::CALLED) |
               (1ULL << PrestoSqlParser::CASCADE) |
               (1ULL << PrestoSqlParser::CASE) |
               (1ULL << PrestoSqlParser::CAST) |
               (1ULL << PrestoSqlParser::CATALOGS) |
               (1ULL << PrestoSqlParser::COLUMN) |
               (1ULL << PrestoSqlParser::COLUMNS) |
               (1ULL << PrestoSqlParser::COMMENT) |
               (1ULL << PrestoSqlParser::COMMIT) |
               (1ULL << PrestoSqlParser::COMMITTED) |
               (1ULL << PrestoSqlParser::CURRENT) |
               (1ULL << PrestoSqlParser::CURRENT_DATE) |
               (1ULL << PrestoSqlParser::CURRENT_ROLE) |
               (1ULL << PrestoSqlParser::CURRENT_TIME) |
               (1ULL << PrestoSqlParser::CURRENT_TIMESTAMP) |
               (1ULL << PrestoSqlParser::CURRENT_USER) |
               (1ULL << PrestoSqlParser::DATA) |
               (1ULL << PrestoSqlParser::DATE) |
               (1ULL << PrestoSqlParser::DAY) |
               (1ULL << PrestoSqlParser::DEFINER) |
               (1ULL << PrestoSqlParser::DESC) |
               (1ULL << PrestoSqlParser::DETERMINISTIC) |
               (1ULL << PrestoSqlParser::DISABLED) |
               (1ULL << PrestoSqlParser::DISTRIBUTED) |
               (1ULL << PrestoSqlParser::ENABLED) |
               (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) &
              ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::EXISTS - 66)) |
               (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
               (1ULL << (PrestoSqlParser::EXTRACT - 66)) |
               (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
               (1ULL << (PrestoSqlParser::FALSE - 66)) |
               (1ULL << (PrestoSqlParser::FETCH - 66)) |
               (1ULL << (PrestoSqlParser::FILTER - 66)) |
               (1ULL << (PrestoSqlParser::FIRST - 66)) |
               (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
               (1ULL << (PrestoSqlParser::FORMAT - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
               (1ULL << (PrestoSqlParser::GRANT - 66)) |
               (1ULL << (PrestoSqlParser::GRANTED - 66)) |
               (1ULL << (PrestoSqlParser::GRANTS - 66)) |
               (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
               (1ULL << (PrestoSqlParser::GROUPING - 66)) |
               (1ULL << (PrestoSqlParser::GROUPS - 66)) |
               (1ULL << (PrestoSqlParser::HOUR - 66)) |
               (1ULL << (PrestoSqlParser::IF - 66)) |
               (1ULL << (PrestoSqlParser::IGNORE - 66)) |
               (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::INPUT - 66)) |
               (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
               (1ULL << (PrestoSqlParser::INVOKER - 66)) |
               (1ULL << (PrestoSqlParser::IO - 66)) |
               (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
               (1ULL << (PrestoSqlParser::JSON - 66)) |
               (1ULL << (PrestoSqlParser::KEY - 66)) |
               (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
               (1ULL << (PrestoSqlParser::LAST - 66)) |
               (1ULL << (PrestoSqlParser::LATERAL - 66)) |
               (1ULL << (PrestoSqlParser::LEVEL - 66)) |
               (1ULL << (PrestoSqlParser::LIMIT - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIME - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIMESTAMP - 66)) |
               (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
               (1ULL << (PrestoSqlParser::MAP - 66)) |
               (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
               (1ULL << (PrestoSqlParser::MINUTE - 66)) |
               (1ULL << (PrestoSqlParser::MONTH - 66)) |
               (1ULL << (PrestoSqlParser::NAME - 66)) |
               (1ULL << (PrestoSqlParser::NFC - 66)) |
               (1ULL << (PrestoSqlParser::NFD - 66)) |
               (1ULL << (PrestoSqlParser::NFKC - 66)) |
               (1ULL << (PrestoSqlParser::NFKD - 66)) |
               (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) &
              ((1ULL << (PrestoSqlParser::NONE - 130)) |
               (1ULL << (PrestoSqlParser::NORMALIZE - 130)) |
               (1ULL << (PrestoSqlParser::NOT - 130)) |
               (1ULL << (PrestoSqlParser::NULL_LITERAL - 130)) |
               (1ULL << (PrestoSqlParser::NULLIF - 130)) |
               (1ULL << (PrestoSqlParser::NULLS - 130)) |
               (1ULL << (PrestoSqlParser::OF - 130)) |
               (1ULL << (PrestoSqlParser::OFFSET - 130)) |
               (1ULL << (PrestoSqlParser::ONLY - 130)) |
               (1ULL << (PrestoSqlParser::OPTION - 130)) |
               (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
               (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
               (1ULL << (PrestoSqlParser::OVER - 130)) |
               (1ULL << (PrestoSqlParser::PARTITION - 130)) |
               (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
               (1ULL << (PrestoSqlParser::POSITION - 130)) |
               (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
               (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
               (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
               (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
               (1ULL << (PrestoSqlParser::RANGE - 130)) |
               (1ULL << (PrestoSqlParser::READ - 130)) |
               (1ULL << (PrestoSqlParser::REFRESH - 130)) |
               (1ULL << (PrestoSqlParser::RELY - 130)) |
               (1ULL << (PrestoSqlParser::RENAME - 130)) |
               (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
               (1ULL << (PrestoSqlParser::REPLACE - 130)) |
               (1ULL << (PrestoSqlParser::RESET - 130)) |
               (1ULL << (PrestoSqlParser::RESPECT - 130)) |
               (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
               (1ULL << (PrestoSqlParser::RETURN - 130)) |
               (1ULL << (PrestoSqlParser::RETURNS - 130)) |
               (1ULL << (PrestoSqlParser::REVOKE - 130)) |
               (1ULL << (PrestoSqlParser::ROLE - 130)) |
               (1ULL << (PrestoSqlParser::ROLES - 130)) |
               (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
               (1ULL << (PrestoSqlParser::ROW - 130)) |
               (1ULL << (PrestoSqlParser::ROWS - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
               (1ULL << (PrestoSqlParser::SECOND - 130)) |
               (1ULL << (PrestoSqlParser::SECURITY - 130)) |
               (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
               (1ULL << (PrestoSqlParser::SESSION - 130)) |
               (1ULL << (PrestoSqlParser::SET - 130)) |
               (1ULL << (PrestoSqlParser::SETS - 130)) |
               (1ULL << (PrestoSqlParser::SHOW - 130)) |
               (1ULL << (PrestoSqlParser::SOME - 130)) |
               (1ULL << (PrestoSqlParser::SQL - 130)) |
               (1ULL << (PrestoSqlParser::START - 130)) |
               (1ULL << (PrestoSqlParser::STATS - 130)) |
               (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
            ((((_la - 195) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 195)) &
              ((1ULL << (PrestoSqlParser::TABLES - 195)) |
               (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
               (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
               (1ULL << (PrestoSqlParser::TEXT - 195)) |
               (1ULL << (PrestoSqlParser::TIME - 195)) |
               (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
               (1ULL << (PrestoSqlParser::TO - 195)) |
               (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
               (1ULL << (PrestoSqlParser::TRUE - 195)) |
               (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
               (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
               (1ULL << (PrestoSqlParser::TYPE - 195)) |
               (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
               (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
               (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
               (1ULL << (PrestoSqlParser::UPDATE - 195)) |
               (1ULL << (PrestoSqlParser::USE - 195)) |
               (1ULL << (PrestoSqlParser::USER - 195)) |
               (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
               (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
               (1ULL << (PrestoSqlParser::VERSION - 195)) |
               (1ULL << (PrestoSqlParser::VIEW - 195)) |
               (1ULL << (PrestoSqlParser::WORK - 195)) |
               (1ULL << (PrestoSqlParser::WRITE - 195)) |
               (1ULL << (PrestoSqlParser::YEAR - 195)) |
               (1ULL << (PrestoSqlParser::ZONE - 195)) |
               (1ULL << (PrestoSqlParser::PLUS - 195)) |
               (1ULL << (PrestoSqlParser::MINUS - 195)) |
               (1ULL << (PrestoSqlParser::STRING - 195)) |
               (1ULL << (PrestoSqlParser::UNICODE_STRING - 195)) |
               (1ULL << (PrestoSqlParser::BINARY_LITERAL - 195)) |
               (1ULL << (PrestoSqlParser::INTEGER_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DECIMAL_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DIGIT_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::QUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::BACKQUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_PRECISION - 195)))) != 0)) {
          setState(587);
          callArgument();
          setState(592);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(588);
            match(PrestoSqlParser::T__3);
            setState(589);
            callArgument();
            setState(594);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(597);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 34: {
        _localctx = _tracker.createInstance<PrestoSqlParser::CreateRoleContext>(
            _localctx);
        enterOuterAlt(_localctx, 34);
        setState(599);
        match(PrestoSqlParser::CREATE);
        setState(600);
        match(PrestoSqlParser::ROLE);
        setState(601);
        antlrcpp::downCast<CreateRoleContext*>(_localctx)->name = identifier();
        setState(605);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(602);
          match(PrestoSqlParser::WITH);
          setState(603);
          match(PrestoSqlParser::ADMIN);
          setState(604);
          grantor();
        }
        break;
      }

      case 35: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DropRoleContext>(
            _localctx);
        enterOuterAlt(_localctx, 35);
        setState(607);
        match(PrestoSqlParser::DROP);
        setState(608);
        match(PrestoSqlParser::ROLE);
        setState(609);
        antlrcpp::downCast<DropRoleContext*>(_localctx)->name = identifier();
        break;
      }

      case 36: {
        _localctx = _tracker.createInstance<PrestoSqlParser::GrantRolesContext>(
            _localctx);
        enterOuterAlt(_localctx, 36);
        setState(610);
        match(PrestoSqlParser::GRANT);
        setState(611);
        roles();
        setState(612);
        match(PrestoSqlParser::TO);
        setState(613);
        principal();
        setState(618);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(614);
          match(PrestoSqlParser::T__3);
          setState(615);
          principal();
          setState(620);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(624);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(621);
          match(PrestoSqlParser::WITH);
          setState(622);
          match(PrestoSqlParser::ADMIN);
          setState(623);
          match(PrestoSqlParser::OPTION);
        }
        setState(629);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::GRANTED) {
          setState(626);
          match(PrestoSqlParser::GRANTED);
          setState(627);
          match(PrestoSqlParser::BY);
          setState(628);
          grantor();
        }
        break;
      }

      case 37: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RevokeRolesContext>(
                _localctx);
        enterOuterAlt(_localctx, 37);
        setState(631);
        match(PrestoSqlParser::REVOKE);
        setState(635);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 60, _ctx)) {
          case 1: {
            setState(632);
            match(PrestoSqlParser::ADMIN);
            setState(633);
            match(PrestoSqlParser::OPTION);
            setState(634);
            match(PrestoSqlParser::FOR);
            break;
          }

          default:
            break;
        }
        setState(637);
        roles();
        setState(638);
        match(PrestoSqlParser::FROM);
        setState(639);
        principal();
        setState(644);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(640);
          match(PrestoSqlParser::T__3);
          setState(641);
          principal();
          setState(646);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(650);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::GRANTED) {
          setState(647);
          match(PrestoSqlParser::GRANTED);
          setState(648);
          match(PrestoSqlParser::BY);
          setState(649);
          grantor();
        }
        break;
      }

      case 38: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SetRoleContext>(_localctx);
        enterOuterAlt(_localctx, 38);
        setState(652);
        match(PrestoSqlParser::SET);
        setState(653);
        match(PrestoSqlParser::ROLE);
        setState(657);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 63, _ctx)) {
          case 1: {
            setState(654);
            match(PrestoSqlParser::ALL);
            break;
          }

          case 2: {
            setState(655);
            match(PrestoSqlParser::NONE);
            break;
          }

          case 3: {
            setState(656);
            antlrcpp::downCast<SetRoleContext*>(_localctx)->role = identifier();
            break;
          }

          default:
            break;
        }
        break;
      }

      case 39: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::GrantContext>(_localctx);
        enterOuterAlt(_localctx, 39);
        setState(659);
        match(PrestoSqlParser::GRANT);
        setState(670);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 65, _ctx)) {
          case 1: {
            setState(660);
            privilege();
            setState(665);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == PrestoSqlParser::T__3) {
              setState(661);
              match(PrestoSqlParser::T__3);
              setState(662);
              privilege();
              setState(667);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
            break;
          }

          case 2: {
            setState(668);
            match(PrestoSqlParser::ALL);
            setState(669);
            match(PrestoSqlParser::PRIVILEGES);
            break;
          }

          default:
            break;
        }
        setState(672);
        match(PrestoSqlParser::ON);
        setState(674);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::TABLE) {
          setState(673);
          match(PrestoSqlParser::TABLE);
        }
        setState(676);
        qualifiedName();
        setState(677);
        match(PrestoSqlParser::TO);
        setState(678);
        antlrcpp::downCast<GrantContext*>(_localctx)->grantee = principal();
        setState(682);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(679);
          match(PrestoSqlParser::WITH);
          setState(680);
          match(PrestoSqlParser::GRANT);
          setState(681);
          match(PrestoSqlParser::OPTION);
        }
        break;
      }

      case 40: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RevokeContext>(_localctx);
        enterOuterAlt(_localctx, 40);
        setState(684);
        match(PrestoSqlParser::REVOKE);
        setState(688);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 68, _ctx)) {
          case 1: {
            setState(685);
            match(PrestoSqlParser::GRANT);
            setState(686);
            match(PrestoSqlParser::OPTION);
            setState(687);
            match(PrestoSqlParser::FOR);
            break;
          }

          default:
            break;
        }
        setState(700);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 70, _ctx)) {
          case 1: {
            setState(690);
            privilege();
            setState(695);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == PrestoSqlParser::T__3) {
              setState(691);
              match(PrestoSqlParser::T__3);
              setState(692);
              privilege();
              setState(697);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
            break;
          }

          case 2: {
            setState(698);
            match(PrestoSqlParser::ALL);
            setState(699);
            match(PrestoSqlParser::PRIVILEGES);
            break;
          }

          default:
            break;
        }
        setState(702);
        match(PrestoSqlParser::ON);
        setState(704);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::TABLE) {
          setState(703);
          match(PrestoSqlParser::TABLE);
        }
        setState(706);
        qualifiedName();
        setState(707);
        match(PrestoSqlParser::FROM);
        setState(708);
        antlrcpp::downCast<RevokeContext*>(_localctx)->grantee = principal();
        break;
      }

      case 41: {
        _localctx = _tracker.createInstance<PrestoSqlParser::ShowGrantsContext>(
            _localctx);
        enterOuterAlt(_localctx, 41);
        setState(710);
        match(PrestoSqlParser::SHOW);
        setState(711);
        match(PrestoSqlParser::GRANTS);
        setState(717);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::ON) {
          setState(712);
          match(PrestoSqlParser::ON);
          setState(714);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::TABLE) {
            setState(713);
            match(PrestoSqlParser::TABLE);
          }
          setState(716);
          qualifiedName();
        }
        break;
      }

      case 42: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ExplainContext>(_localctx);
        enterOuterAlt(_localctx, 42);
        setState(719);
        match(PrestoSqlParser::EXPLAIN);
        setState(721);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 74, _ctx)) {
          case 1: {
            setState(720);
            match(PrestoSqlParser::ANALYZE);
            break;
          }

          default:
            break;
        }
        setState(724);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::VERBOSE) {
          setState(723);
          match(PrestoSqlParser::VERBOSE);
        }
        setState(737);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 77, _ctx)) {
          case 1: {
            setState(726);
            match(PrestoSqlParser::T__1);
            setState(727);
            explainOption();
            setState(732);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == PrestoSqlParser::T__3) {
              setState(728);
              match(PrestoSqlParser::T__3);
              setState(729);
              explainOption();
              setState(734);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
            setState(735);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        setState(739);
        statement();
        break;
      }

      case 43: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowCreateTableContext>(
                _localctx);
        enterOuterAlt(_localctx, 43);
        setState(740);
        match(PrestoSqlParser::SHOW);
        setState(741);
        match(PrestoSqlParser::CREATE);
        setState(742);
        match(PrestoSqlParser::TABLE);
        setState(743);
        qualifiedName();
        break;
      }

      case 44: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowCreateViewContext>(
                _localctx);
        enterOuterAlt(_localctx, 44);
        setState(744);
        match(PrestoSqlParser::SHOW);
        setState(745);
        match(PrestoSqlParser::CREATE);
        setState(746);
        match(PrestoSqlParser::VIEW);
        setState(747);
        qualifiedName();
        break;
      }

      case 45: {
        _localctx = _tracker.createInstance<
            PrestoSqlParser::ShowCreateMaterializedViewContext>(_localctx);
        enterOuterAlt(_localctx, 45);
        setState(748);
        match(PrestoSqlParser::SHOW);
        setState(749);
        match(PrestoSqlParser::CREATE);
        setState(750);
        match(PrestoSqlParser::MATERIALIZED);
        setState(751);
        match(PrestoSqlParser::VIEW);
        setState(752);
        qualifiedName();
        break;
      }

      case 46: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowCreateFunctionContext>(
                _localctx);
        enterOuterAlt(_localctx, 46);
        setState(753);
        match(PrestoSqlParser::SHOW);
        setState(754);
        match(PrestoSqlParser::CREATE);
        setState(755);
        match(PrestoSqlParser::FUNCTION);
        setState(756);
        qualifiedName();
        setState(758);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::T__1) {
          setState(757);
          types();
        }
        break;
      }

      case 47: {
        _localctx = _tracker.createInstance<PrestoSqlParser::ShowTablesContext>(
            _localctx);
        enterOuterAlt(_localctx, 47);
        setState(760);
        match(PrestoSqlParser::SHOW);
        setState(761);
        match(PrestoSqlParser::TABLES);
        setState(764);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::FROM

            || _la == PrestoSqlParser::IN) {
          setState(762);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::FROM

                || _la == PrestoSqlParser::IN)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(763);
          qualifiedName();
        }
        setState(772);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::LIKE) {
          setState(766);
          match(PrestoSqlParser::LIKE);
          setState(767);
          antlrcpp::downCast<ShowTablesContext*>(_localctx)->pattern = string();
          setState(770);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::ESCAPE) {
            setState(768);
            match(PrestoSqlParser::ESCAPE);
            setState(769);
            antlrcpp::downCast<ShowTablesContext*>(_localctx)->escape =
                string();
          }
        }
        break;
      }

      case 48: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowSchemasContext>(
                _localctx);
        enterOuterAlt(_localctx, 48);
        setState(774);
        match(PrestoSqlParser::SHOW);
        setState(775);
        match(PrestoSqlParser::SCHEMAS);
        setState(778);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::FROM

            || _la == PrestoSqlParser::IN) {
          setState(776);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::FROM

                || _la == PrestoSqlParser::IN)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(777);
          identifier();
        }
        setState(786);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::LIKE) {
          setState(780);
          match(PrestoSqlParser::LIKE);
          setState(781);
          antlrcpp::downCast<ShowSchemasContext*>(_localctx)->pattern =
              string();
          setState(784);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::ESCAPE) {
            setState(782);
            match(PrestoSqlParser::ESCAPE);
            setState(783);
            antlrcpp::downCast<ShowSchemasContext*>(_localctx)->escape =
                string();
          }
        }
        break;
      }

      case 49: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowCatalogsContext>(
                _localctx);
        enterOuterAlt(_localctx, 49);
        setState(788);
        match(PrestoSqlParser::SHOW);
        setState(789);
        match(PrestoSqlParser::CATALOGS);
        setState(796);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::LIKE) {
          setState(790);
          match(PrestoSqlParser::LIKE);
          setState(791);
          antlrcpp::downCast<ShowCatalogsContext*>(_localctx)->pattern =
              string();
          setState(794);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::ESCAPE) {
            setState(792);
            match(PrestoSqlParser::ESCAPE);
            setState(793);
            antlrcpp::downCast<ShowCatalogsContext*>(_localctx)->escape =
                string();
          }
        }
        break;
      }

      case 50: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowColumnsContext>(
                _localctx);
        enterOuterAlt(_localctx, 50);
        setState(798);
        match(PrestoSqlParser::SHOW);
        setState(799);
        match(PrestoSqlParser::COLUMNS);
        setState(800);
        _la = _input->LA(1);
        if (!(_la == PrestoSqlParser::FROM

              || _la == PrestoSqlParser::IN)) {
          _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(801);
        qualifiedName();
        break;
      }

      case 51: {
        _localctx = _tracker.createInstance<PrestoSqlParser::ShowStatsContext>(
            _localctx);
        enterOuterAlt(_localctx, 51);
        setState(802);
        match(PrestoSqlParser::SHOW);
        setState(803);
        match(PrestoSqlParser::STATS);
        setState(804);
        match(PrestoSqlParser::FOR);
        setState(805);
        qualifiedName();
        break;
      }

      case 52: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowStatsForQueryContext>(
                _localctx);
        enterOuterAlt(_localctx, 52);
        setState(806);
        match(PrestoSqlParser::SHOW);
        setState(807);
        match(PrestoSqlParser::STATS);
        setState(808);
        match(PrestoSqlParser::FOR);
        setState(809);
        match(PrestoSqlParser::T__1);
        setState(810);
        querySpecification();
        setState(811);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 53: {
        _localctx = _tracker.createInstance<PrestoSqlParser::ShowRolesContext>(
            _localctx);
        enterOuterAlt(_localctx, 53);
        setState(813);
        match(PrestoSqlParser::SHOW);
        setState(815);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::CURRENT) {
          setState(814);
          match(PrestoSqlParser::CURRENT);
        }
        setState(817);
        match(PrestoSqlParser::ROLES);
        setState(820);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::FROM

            || _la == PrestoSqlParser::IN) {
          setState(818);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::FROM

                || _la == PrestoSqlParser::IN)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(819);
          identifier();
        }
        break;
      }

      case 54: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowRoleGrantsContext>(
                _localctx);
        enterOuterAlt(_localctx, 54);
        setState(822);
        match(PrestoSqlParser::SHOW);
        setState(823);
        match(PrestoSqlParser::ROLE);
        setState(824);
        match(PrestoSqlParser::GRANTS);
        setState(827);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::FROM

            || _la == PrestoSqlParser::IN) {
          setState(825);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::FROM

                || _la == PrestoSqlParser::IN)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(826);
          identifier();
        }
        break;
      }

      case 55: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowColumnsContext>(
                _localctx);
        enterOuterAlt(_localctx, 55);
        setState(829);
        match(PrestoSqlParser::DESCRIBE);
        setState(830);
        qualifiedName();
        break;
      }

      case 56: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowColumnsContext>(
                _localctx);
        enterOuterAlt(_localctx, 56);
        setState(831);
        match(PrestoSqlParser::DESC);
        setState(832);
        qualifiedName();
        break;
      }

      case 57: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowFunctionsContext>(
                _localctx);
        enterOuterAlt(_localctx, 57);
        setState(833);
        match(PrestoSqlParser::SHOW);
        setState(834);
        match(PrestoSqlParser::FUNCTIONS);
        setState(841);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::LIKE) {
          setState(835);
          match(PrestoSqlParser::LIKE);
          setState(836);
          antlrcpp::downCast<ShowFunctionsContext*>(_localctx)->pattern =
              string();
          setState(839);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::ESCAPE) {
            setState(837);
            match(PrestoSqlParser::ESCAPE);
            setState(838);
            antlrcpp::downCast<ShowFunctionsContext*>(_localctx)->escape =
                string();
          }
        }
        break;
      }

      case 58: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowSessionContext>(
                _localctx);
        enterOuterAlt(_localctx, 58);
        setState(843);
        match(PrestoSqlParser::SHOW);
        setState(844);
        match(PrestoSqlParser::SESSION);
        setState(851);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::LIKE) {
          setState(845);
          match(PrestoSqlParser::LIKE);
          setState(846);
          antlrcpp::downCast<ShowSessionContext*>(_localctx)->pattern =
              string();
          setState(849);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::ESCAPE) {
            setState(847);
            match(PrestoSqlParser::ESCAPE);
            setState(848);
            antlrcpp::downCast<ShowSessionContext*>(_localctx)->escape =
                string();
          }
        }
        break;
      }

      case 59: {
        _localctx = _tracker.createInstance<PrestoSqlParser::SetSessionContext>(
            _localctx);
        enterOuterAlt(_localctx, 59);
        setState(853);
        match(PrestoSqlParser::SET);
        setState(854);
        match(PrestoSqlParser::SESSION);
        setState(855);
        qualifiedName();
        setState(856);
        match(PrestoSqlParser::EQ);
        setState(857);
        expression();
        break;
      }

      case 60: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ResetSessionContext>(
                _localctx);
        enterOuterAlt(_localctx, 60);
        setState(859);
        match(PrestoSqlParser::RESET);
        setState(860);
        match(PrestoSqlParser::SESSION);
        setState(861);
        qualifiedName();
        break;
      }

      case 61: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::StartTransactionContext>(
                _localctx);
        enterOuterAlt(_localctx, 61);
        setState(862);
        match(PrestoSqlParser::START);
        setState(863);
        match(PrestoSqlParser::TRANSACTION);
        setState(872);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::ISOLATION

            || _la == PrestoSqlParser::READ) {
          setState(864);
          transactionMode();
          setState(869);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(865);
            match(PrestoSqlParser::T__3);
            setState(866);
            transactionMode();
            setState(871);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        break;
      }

      case 62: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CommitContext>(_localctx);
        enterOuterAlt(_localctx, 62);
        setState(874);
        match(PrestoSqlParser::COMMIT);
        setState(876);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WORK) {
          setState(875);
          match(PrestoSqlParser::WORK);
        }
        break;
      }

      case 63: {
        _localctx = _tracker.createInstance<PrestoSqlParser::RollbackContext>(
            _localctx);
        enterOuterAlt(_localctx, 63);
        setState(878);
        match(PrestoSqlParser::ROLLBACK);
        setState(880);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WORK) {
          setState(879);
          match(PrestoSqlParser::WORK);
        }
        break;
      }

      case 64: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::PrepareContext>(_localctx);
        enterOuterAlt(_localctx, 64);
        setState(882);
        match(PrestoSqlParser::PREPARE);
        setState(883);
        identifier();
        setState(884);
        match(PrestoSqlParser::FROM);
        setState(885);
        statement();
        break;
      }

      case 65: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DeallocateContext>(
            _localctx);
        enterOuterAlt(_localctx, 65);
        setState(887);
        match(PrestoSqlParser::DEALLOCATE);
        setState(888);
        match(PrestoSqlParser::PREPARE);
        setState(889);
        identifier();
        break;
      }

      case 66: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ExecuteContext>(_localctx);
        enterOuterAlt(_localctx, 66);
        setState(890);
        match(PrestoSqlParser::EXECUTE);
        setState(891);
        identifier();
        setState(901);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::USING) {
          setState(892);
          match(PrestoSqlParser::USING);
          setState(893);
          expression();
          setState(898);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(894);
            match(PrestoSqlParser::T__3);
            setState(895);
            expression();
            setState(900);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        break;
      }

      case 67: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DescribeInputContext>(
                _localctx);
        enterOuterAlt(_localctx, 67);
        setState(903);
        match(PrestoSqlParser::DESCRIBE);
        setState(904);
        match(PrestoSqlParser::INPUT);
        setState(905);
        identifier();
        break;
      }

      case 68: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DescribeOutputContext>(
                _localctx);
        enterOuterAlt(_localctx, 68);
        setState(906);
        match(PrestoSqlParser::DESCRIBE);
        setState(907);
        match(PrestoSqlParser::OUTPUT);
        setState(908);
        identifier();
        break;
      }

      case 69: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UpdateContext>(_localctx);
        enterOuterAlt(_localctx, 69);
        setState(909);
        match(PrestoSqlParser::UPDATE);
        setState(910);
        qualifiedName();
        setState(911);
        match(PrestoSqlParser::SET);
        setState(912);
        updateAssignment();
        setState(917);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(913);
          match(PrestoSqlParser::T__3);
          setState(914);
          updateAssignment();
          setState(919);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(922);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WHERE) {
          setState(920);
          match(PrestoSqlParser::WHERE);
          setState(921);
          antlrcpp::downCast<UpdateContext*>(_localctx)->where =
              booleanExpression(0);
        }
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QueryContext
//------------------------------------------------------------------

PrestoSqlParser::QueryContext::QueryContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::QueryNoWithContext*
PrestoSqlParser::QueryContext::queryNoWith() {
  return getRuleContext<PrestoSqlParser::QueryNoWithContext>(0);
}

PrestoSqlParser::WithContext* PrestoSqlParser::QueryContext::with() {
  return getRuleContext<PrestoSqlParser::WithContext>(0);
}

size_t PrestoSqlParser::QueryContext::getRuleIndex() const {
  return PrestoSqlParser::RuleQuery;
}

void PrestoSqlParser::QueryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuery(this);
}

void PrestoSqlParser::QueryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuery(this);
}

antlrcpp::Any PrestoSqlParser::QueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitQuery(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::QueryContext* PrestoSqlParser::query() {
  QueryContext* _localctx =
      _tracker.createInstance<QueryContext>(_ctx, getState());
  enterRule(_localctx, 8, PrestoSqlParser::RuleQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(927);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::WITH) {
      setState(926);
      with();
    }
    setState(929);
    queryNoWith();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WithContext
//------------------------------------------------------------------

PrestoSqlParser::WithContext::WithContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::WithContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

std::vector<PrestoSqlParser::NamedQueryContext*>
PrestoSqlParser::WithContext::namedQuery() {
  return getRuleContexts<PrestoSqlParser::NamedQueryContext>();
}

PrestoSqlParser::NamedQueryContext* PrestoSqlParser::WithContext::namedQuery(
    size_t i) {
  return getRuleContext<PrestoSqlParser::NamedQueryContext>(i);
}

tree::TerminalNode* PrestoSqlParser::WithContext::RECURSIVE() {
  return getToken(PrestoSqlParser::RECURSIVE, 0);
}

size_t PrestoSqlParser::WithContext::getRuleIndex() const {
  return PrestoSqlParser::RuleWith;
}

void PrestoSqlParser::WithContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterWith(this);
}

void PrestoSqlParser::WithContext::exitRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitWith(this);
}

antlrcpp::Any PrestoSqlParser::WithContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitWith(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::WithContext* PrestoSqlParser::with() {
  WithContext* _localctx =
      _tracker.createInstance<WithContext>(_ctx, getState());
  enterRule(_localctx, 10, PrestoSqlParser::RuleWith);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(931);
    match(PrestoSqlParser::WITH);
    setState(933);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::RECURSIVE) {
      setState(932);
      match(PrestoSqlParser::RECURSIVE);
    }
    setState(935);
    namedQuery();
    setState(940);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::T__3) {
      setState(936);
      match(PrestoSqlParser::T__3);
      setState(937);
      namedQuery();
      setState(942);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableElementContext
//------------------------------------------------------------------

PrestoSqlParser::TableElementContext::TableElementContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::ConstraintSpecificationContext*
PrestoSqlParser::TableElementContext::constraintSpecification() {
  return getRuleContext<PrestoSqlParser::ConstraintSpecificationContext>(0);
}

PrestoSqlParser::ColumnDefinitionContext*
PrestoSqlParser::TableElementContext::columnDefinition() {
  return getRuleContext<PrestoSqlParser::ColumnDefinitionContext>(0);
}

PrestoSqlParser::LikeClauseContext*
PrestoSqlParser::TableElementContext::likeClause() {
  return getRuleContext<PrestoSqlParser::LikeClauseContext>(0);
}

size_t PrestoSqlParser::TableElementContext::getRuleIndex() const {
  return PrestoSqlParser::RuleTableElement;
}

void PrestoSqlParser::TableElementContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTableElement(this);
}

void PrestoSqlParser::TableElementContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTableElement(this);
}

antlrcpp::Any PrestoSqlParser::TableElementContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTableElement(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::TableElementContext* PrestoSqlParser::tableElement() {
  TableElementContext* _localctx =
      _tracker.createInstance<TableElementContext>(_ctx, getState());
  enterRule(_localctx, 12, PrestoSqlParser::RuleTableElement);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(946);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 106, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(943);
        constraintSpecification();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(944);
        columnDefinition();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(945);
        likeClause();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnDefinitionContext
//------------------------------------------------------------------

PrestoSqlParser::ColumnDefinitionContext::ColumnDefinitionContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ColumnDefinitionContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::TypeContext* PrestoSqlParser::ColumnDefinitionContext::type() {
  return getRuleContext<PrestoSqlParser::TypeContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ColumnDefinitionContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

tree::TerminalNode* PrestoSqlParser::ColumnDefinitionContext::NULL_LITERAL() {
  return getToken(PrestoSqlParser::NULL_LITERAL, 0);
}

tree::TerminalNode* PrestoSqlParser::ColumnDefinitionContext::COMMENT() {
  return getToken(PrestoSqlParser::COMMENT, 0);
}

PrestoSqlParser::StringContext*
PrestoSqlParser::ColumnDefinitionContext::string() {
  return getRuleContext<PrestoSqlParser::StringContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ColumnDefinitionContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

PrestoSqlParser::PropertiesContext*
PrestoSqlParser::ColumnDefinitionContext::properties() {
  return getRuleContext<PrestoSqlParser::PropertiesContext>(0);
}

size_t PrestoSqlParser::ColumnDefinitionContext::getRuleIndex() const {
  return PrestoSqlParser::RuleColumnDefinition;
}

void PrestoSqlParser::ColumnDefinitionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterColumnDefinition(this);
}

void PrestoSqlParser::ColumnDefinitionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitColumnDefinition(this);
}

antlrcpp::Any PrestoSqlParser::ColumnDefinitionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitColumnDefinition(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ColumnDefinitionContext* PrestoSqlParser::columnDefinition() {
  ColumnDefinitionContext* _localctx =
      _tracker.createInstance<ColumnDefinitionContext>(_ctx, getState());
  enterRule(_localctx, 14, PrestoSqlParser::RuleColumnDefinition);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(948);
    identifier();
    setState(949);
    type(0);
    setState(952);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::NOT) {
      setState(950);
      match(PrestoSqlParser::NOT);
      setState(951);
      match(PrestoSqlParser::NULL_LITERAL);
    }
    setState(956);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::COMMENT) {
      setState(954);
      match(PrestoSqlParser::COMMENT);
      setState(955);
      string();
    }
    setState(960);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::WITH) {
      setState(958);
      match(PrestoSqlParser::WITH);
      setState(959);
      properties();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LikeClauseContext
//------------------------------------------------------------------

PrestoSqlParser::LikeClauseContext::LikeClauseContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::LikeClauseContext::LIKE() {
  return getToken(PrestoSqlParser::LIKE, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::LikeClauseContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::LikeClauseContext::PROPERTIES() {
  return getToken(PrestoSqlParser::PROPERTIES, 0);
}

tree::TerminalNode* PrestoSqlParser::LikeClauseContext::INCLUDING() {
  return getToken(PrestoSqlParser::INCLUDING, 0);
}

tree::TerminalNode* PrestoSqlParser::LikeClauseContext::EXCLUDING() {
  return getToken(PrestoSqlParser::EXCLUDING, 0);
}

size_t PrestoSqlParser::LikeClauseContext::getRuleIndex() const {
  return PrestoSqlParser::RuleLikeClause;
}

void PrestoSqlParser::LikeClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterLikeClause(this);
}

void PrestoSqlParser::LikeClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitLikeClause(this);
}

antlrcpp::Any PrestoSqlParser::LikeClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitLikeClause(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::LikeClauseContext* PrestoSqlParser::likeClause() {
  LikeClauseContext* _localctx =
      _tracker.createInstance<LikeClauseContext>(_ctx, getState());
  enterRule(_localctx, 16, PrestoSqlParser::RuleLikeClause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(962);
    match(PrestoSqlParser::LIKE);
    setState(963);
    qualifiedName();
    setState(966);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::EXCLUDING

        || _la == PrestoSqlParser::INCLUDING) {
      setState(964);
      antlrcpp::downCast<LikeClauseContext*>(_localctx)->optionType =
          _input->LT(1);
      _la = _input->LA(1);
      if (!(_la == PrestoSqlParser::EXCLUDING

            || _la == PrestoSqlParser::INCLUDING)) {
        antlrcpp::downCast<LikeClauseContext*>(_localctx)->optionType =
            _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
      setState(965);
      match(PrestoSqlParser::PROPERTIES);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertiesContext
//------------------------------------------------------------------

PrestoSqlParser::PropertiesContext::PropertiesContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::PropertyContext*>
PrestoSqlParser::PropertiesContext::property() {
  return getRuleContexts<PrestoSqlParser::PropertyContext>();
}

PrestoSqlParser::PropertyContext* PrestoSqlParser::PropertiesContext::property(
    size_t i) {
  return getRuleContext<PrestoSqlParser::PropertyContext>(i);
}

size_t PrestoSqlParser::PropertiesContext::getRuleIndex() const {
  return PrestoSqlParser::RuleProperties;
}

void PrestoSqlParser::PropertiesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterProperties(this);
}

void PrestoSqlParser::PropertiesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitProperties(this);
}

antlrcpp::Any PrestoSqlParser::PropertiesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitProperties(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::PropertiesContext* PrestoSqlParser::properties() {
  PropertiesContext* _localctx =
      _tracker.createInstance<PropertiesContext>(_ctx, getState());
  enterRule(_localctx, 18, PrestoSqlParser::RuleProperties);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(968);
    match(PrestoSqlParser::T__1);
    setState(969);
    property();
    setState(974);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::T__3) {
      setState(970);
      match(PrestoSqlParser::T__3);
      setState(971);
      property();
      setState(976);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(977);
    match(PrestoSqlParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyContext
//------------------------------------------------------------------

PrestoSqlParser::PropertyContext::PropertyContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::PropertyContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::PropertyContext::EQ() {
  return getToken(PrestoSqlParser::EQ, 0);
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::PropertyContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

size_t PrestoSqlParser::PropertyContext::getRuleIndex() const {
  return PrestoSqlParser::RuleProperty;
}

void PrestoSqlParser::PropertyContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterProperty(this);
}

void PrestoSqlParser::PropertyContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitProperty(this);
}

antlrcpp::Any PrestoSqlParser::PropertyContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitProperty(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::PropertyContext* PrestoSqlParser::property() {
  PropertyContext* _localctx =
      _tracker.createInstance<PropertyContext>(_ctx, getState());
  enterRule(_localctx, 20, PrestoSqlParser::RuleProperty);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(979);
    identifier();
    setState(980);
    match(PrestoSqlParser::EQ);
    setState(981);
    expression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SqlParameterDeclarationContext
//------------------------------------------------------------------

PrestoSqlParser::SqlParameterDeclarationContext::SqlParameterDeclarationContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::SqlParameterDeclarationContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::TypeContext*
PrestoSqlParser::SqlParameterDeclarationContext::type() {
  return getRuleContext<PrestoSqlParser::TypeContext>(0);
}

size_t PrestoSqlParser::SqlParameterDeclarationContext::getRuleIndex() const {
  return PrestoSqlParser::RuleSqlParameterDeclaration;
}

void PrestoSqlParser::SqlParameterDeclarationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSqlParameterDeclaration(this);
}

void PrestoSqlParser::SqlParameterDeclarationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSqlParameterDeclaration(this);
}

antlrcpp::Any PrestoSqlParser::SqlParameterDeclarationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSqlParameterDeclaration(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::SqlParameterDeclarationContext*
PrestoSqlParser::sqlParameterDeclaration() {
  SqlParameterDeclarationContext* _localctx =
      _tracker.createInstance<SqlParameterDeclarationContext>(_ctx, getState());
  enterRule(_localctx, 22, PrestoSqlParser::RuleSqlParameterDeclaration);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(983);
    identifier();
    setState(984);
    type(0);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RoutineCharacteristicsContext
//------------------------------------------------------------------

PrestoSqlParser::RoutineCharacteristicsContext::RoutineCharacteristicsContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::RoutineCharacteristicContext*>
PrestoSqlParser::RoutineCharacteristicsContext::routineCharacteristic() {
  return getRuleContexts<PrestoSqlParser::RoutineCharacteristicContext>();
}

PrestoSqlParser::RoutineCharacteristicContext*
PrestoSqlParser::RoutineCharacteristicsContext::routineCharacteristic(
    size_t i) {
  return getRuleContext<PrestoSqlParser::RoutineCharacteristicContext>(i);
}

size_t PrestoSqlParser::RoutineCharacteristicsContext::getRuleIndex() const {
  return PrestoSqlParser::RuleRoutineCharacteristics;
}

void PrestoSqlParser::RoutineCharacteristicsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRoutineCharacteristics(this);
}

void PrestoSqlParser::RoutineCharacteristicsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRoutineCharacteristics(this);
}

antlrcpp::Any PrestoSqlParser::RoutineCharacteristicsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRoutineCharacteristics(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::RoutineCharacteristicsContext*
PrestoSqlParser::routineCharacteristics() {
  RoutineCharacteristicsContext* _localctx =
      _tracker.createInstance<RoutineCharacteristicsContext>(_ctx, getState());
  enterRule(_localctx, 24, PrestoSqlParser::RuleRoutineCharacteristics);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(989);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::CALLED

           || _la == PrestoSqlParser::DETERMINISTIC ||
           ((((_la - 109) & ~0x3fULL) == 0) &&
            ((1ULL << (_la - 109)) &
             ((1ULL << (PrestoSqlParser::LANGUAGE - 109)) |
              (1ULL << (PrestoSqlParser::NOT - 109)) |
              (1ULL << (PrestoSqlParser::RETURNS - 109)))) != 0)) {
      setState(986);
      routineCharacteristic();
      setState(991);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RoutineCharacteristicContext
//------------------------------------------------------------------

PrestoSqlParser::RoutineCharacteristicContext::RoutineCharacteristicContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::RoutineCharacteristicContext::LANGUAGE() {
  return getToken(PrestoSqlParser::LANGUAGE, 0);
}

PrestoSqlParser::LanguageContext*
PrestoSqlParser::RoutineCharacteristicContext::language() {
  return getRuleContext<PrestoSqlParser::LanguageContext>(0);
}

PrestoSqlParser::DeterminismContext*
PrestoSqlParser::RoutineCharacteristicContext::determinism() {
  return getRuleContext<PrestoSqlParser::DeterminismContext>(0);
}

PrestoSqlParser::NullCallClauseContext*
PrestoSqlParser::RoutineCharacteristicContext::nullCallClause() {
  return getRuleContext<PrestoSqlParser::NullCallClauseContext>(0);
}

size_t PrestoSqlParser::RoutineCharacteristicContext::getRuleIndex() const {
  return PrestoSqlParser::RuleRoutineCharacteristic;
}

void PrestoSqlParser::RoutineCharacteristicContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRoutineCharacteristic(this);
}

void PrestoSqlParser::RoutineCharacteristicContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRoutineCharacteristic(this);
}

antlrcpp::Any PrestoSqlParser::RoutineCharacteristicContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRoutineCharacteristic(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::RoutineCharacteristicContext*
PrestoSqlParser::routineCharacteristic() {
  RoutineCharacteristicContext* _localctx =
      _tracker.createInstance<RoutineCharacteristicContext>(_ctx, getState());
  enterRule(_localctx, 26, PrestoSqlParser::RuleRoutineCharacteristic);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(996);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::LANGUAGE: {
        enterOuterAlt(_localctx, 1);
        setState(992);
        match(PrestoSqlParser::LANGUAGE);
        setState(993);
        language();
        break;
      }

      case PrestoSqlParser::DETERMINISTIC:
      case PrestoSqlParser::NOT: {
        enterOuterAlt(_localctx, 2);
        setState(994);
        determinism();
        break;
      }

      case PrestoSqlParser::CALLED:
      case PrestoSqlParser::RETURNS: {
        enterOuterAlt(_localctx, 3);
        setState(995);
        nullCallClause();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AlterRoutineCharacteristicsContext
//------------------------------------------------------------------

PrestoSqlParser::AlterRoutineCharacteristicsContext::
    AlterRoutineCharacteristicsContext(
        ParserRuleContext* parent,
        size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::AlterRoutineCharacteristicContext*>
PrestoSqlParser::AlterRoutineCharacteristicsContext::
    alterRoutineCharacteristic() {
  return getRuleContexts<PrestoSqlParser::AlterRoutineCharacteristicContext>();
}

PrestoSqlParser::AlterRoutineCharacteristicContext*
PrestoSqlParser::AlterRoutineCharacteristicsContext::alterRoutineCharacteristic(
    size_t i) {
  return getRuleContext<PrestoSqlParser::AlterRoutineCharacteristicContext>(i);
}

size_t PrestoSqlParser::AlterRoutineCharacteristicsContext::getRuleIndex()
    const {
  return PrestoSqlParser::RuleAlterRoutineCharacteristics;
}

void PrestoSqlParser::AlterRoutineCharacteristicsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAlterRoutineCharacteristics(this);
}

void PrestoSqlParser::AlterRoutineCharacteristicsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitAlterRoutineCharacteristics(this);
}

antlrcpp::Any PrestoSqlParser::AlterRoutineCharacteristicsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAlterRoutineCharacteristics(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::AlterRoutineCharacteristicsContext*
PrestoSqlParser::alterRoutineCharacteristics() {
  AlterRoutineCharacteristicsContext* _localctx =
      _tracker.createInstance<AlterRoutineCharacteristicsContext>(
          _ctx, getState());
  enterRule(_localctx, 28, PrestoSqlParser::RuleAlterRoutineCharacteristics);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1001);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::CALLED || _la == PrestoSqlParser::RETURNS) {
      setState(998);
      alterRoutineCharacteristic();
      setState(1003);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AlterRoutineCharacteristicContext
//------------------------------------------------------------------

PrestoSqlParser::AlterRoutineCharacteristicContext::
    AlterRoutineCharacteristicContext(
        ParserRuleContext* parent,
        size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::NullCallClauseContext*
PrestoSqlParser::AlterRoutineCharacteristicContext::nullCallClause() {
  return getRuleContext<PrestoSqlParser::NullCallClauseContext>(0);
}

size_t PrestoSqlParser::AlterRoutineCharacteristicContext::getRuleIndex()
    const {
  return PrestoSqlParser::RuleAlterRoutineCharacteristic;
}

void PrestoSqlParser::AlterRoutineCharacteristicContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAlterRoutineCharacteristic(this);
}

void PrestoSqlParser::AlterRoutineCharacteristicContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitAlterRoutineCharacteristic(this);
}

antlrcpp::Any PrestoSqlParser::AlterRoutineCharacteristicContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAlterRoutineCharacteristic(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::AlterRoutineCharacteristicContext*
PrestoSqlParser::alterRoutineCharacteristic() {
  AlterRoutineCharacteristicContext* _localctx =
      _tracker.createInstance<AlterRoutineCharacteristicContext>(
          _ctx, getState());
  enterRule(_localctx, 30, PrestoSqlParser::RuleAlterRoutineCharacteristic);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1004);
    nullCallClause();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RoutineBodyContext
//------------------------------------------------------------------

PrestoSqlParser::RoutineBodyContext::RoutineBodyContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::ReturnStatementContext*
PrestoSqlParser::RoutineBodyContext::returnStatement() {
  return getRuleContext<PrestoSqlParser::ReturnStatementContext>(0);
}

PrestoSqlParser::ExternalBodyReferenceContext*
PrestoSqlParser::RoutineBodyContext::externalBodyReference() {
  return getRuleContext<PrestoSqlParser::ExternalBodyReferenceContext>(0);
}

size_t PrestoSqlParser::RoutineBodyContext::getRuleIndex() const {
  return PrestoSqlParser::RuleRoutineBody;
}

void PrestoSqlParser::RoutineBodyContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRoutineBody(this);
}

void PrestoSqlParser::RoutineBodyContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRoutineBody(this);
}

antlrcpp::Any PrestoSqlParser::RoutineBodyContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRoutineBody(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::RoutineBodyContext* PrestoSqlParser::routineBody() {
  RoutineBodyContext* _localctx =
      _tracker.createInstance<RoutineBodyContext>(_ctx, getState());
  enterRule(_localctx, 32, PrestoSqlParser::RuleRoutineBody);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1008);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::RETURN: {
        enterOuterAlt(_localctx, 1);
        setState(1006);
        returnStatement();
        break;
      }

      case PrestoSqlParser::EXTERNAL: {
        enterOuterAlt(_localctx, 2);
        setState(1007);
        externalBodyReference();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ReturnStatementContext
//------------------------------------------------------------------

PrestoSqlParser::ReturnStatementContext::ReturnStatementContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::ReturnStatementContext::RETURN() {
  return getToken(PrestoSqlParser::RETURN, 0);
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::ReturnStatementContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

size_t PrestoSqlParser::ReturnStatementContext::getRuleIndex() const {
  return PrestoSqlParser::RuleReturnStatement;
}

void PrestoSqlParser::ReturnStatementContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterReturnStatement(this);
}

void PrestoSqlParser::ReturnStatementContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitReturnStatement(this);
}

antlrcpp::Any PrestoSqlParser::ReturnStatementContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitReturnStatement(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ReturnStatementContext* PrestoSqlParser::returnStatement() {
  ReturnStatementContext* _localctx =
      _tracker.createInstance<ReturnStatementContext>(_ctx, getState());
  enterRule(_localctx, 34, PrestoSqlParser::RuleReturnStatement);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1010);
    match(PrestoSqlParser::RETURN);
    setState(1011);
    expression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExternalBodyReferenceContext
//------------------------------------------------------------------

PrestoSqlParser::ExternalBodyReferenceContext::ExternalBodyReferenceContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::ExternalBodyReferenceContext::EXTERNAL() {
  return getToken(PrestoSqlParser::EXTERNAL, 0);
}

tree::TerminalNode* PrestoSqlParser::ExternalBodyReferenceContext::NAME() {
  return getToken(PrestoSqlParser::NAME, 0);
}

PrestoSqlParser::ExternalRoutineNameContext*
PrestoSqlParser::ExternalBodyReferenceContext::externalRoutineName() {
  return getRuleContext<PrestoSqlParser::ExternalRoutineNameContext>(0);
}

size_t PrestoSqlParser::ExternalBodyReferenceContext::getRuleIndex() const {
  return PrestoSqlParser::RuleExternalBodyReference;
}

void PrestoSqlParser::ExternalBodyReferenceContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterExternalBodyReference(this);
}

void PrestoSqlParser::ExternalBodyReferenceContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitExternalBodyReference(this);
}

antlrcpp::Any PrestoSqlParser::ExternalBodyReferenceContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExternalBodyReference(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ExternalBodyReferenceContext*
PrestoSqlParser::externalBodyReference() {
  ExternalBodyReferenceContext* _localctx =
      _tracker.createInstance<ExternalBodyReferenceContext>(_ctx, getState());
  enterRule(_localctx, 36, PrestoSqlParser::RuleExternalBodyReference);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1013);
    match(PrestoSqlParser::EXTERNAL);
    setState(1016);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::NAME) {
      setState(1014);
      match(PrestoSqlParser::NAME);
      setState(1015);
      externalRoutineName();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LanguageContext
//------------------------------------------------------------------

PrestoSqlParser::LanguageContext::LanguageContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::LanguageContext::SQL() {
  return getToken(PrestoSqlParser::SQL, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::LanguageContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

size_t PrestoSqlParser::LanguageContext::getRuleIndex() const {
  return PrestoSqlParser::RuleLanguage;
}

void PrestoSqlParser::LanguageContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterLanguage(this);
}

void PrestoSqlParser::LanguageContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitLanguage(this);
}

antlrcpp::Any PrestoSqlParser::LanguageContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitLanguage(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::LanguageContext* PrestoSqlParser::language() {
  LanguageContext* _localctx =
      _tracker.createInstance<LanguageContext>(_ctx, getState());
  enterRule(_localctx, 38, PrestoSqlParser::RuleLanguage);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1020);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 117, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(1018);
        match(PrestoSqlParser::SQL);
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(1019);
        identifier();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DeterminismContext
//------------------------------------------------------------------

PrestoSqlParser::DeterminismContext::DeterminismContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::DeterminismContext::DETERMINISTIC() {
  return getToken(PrestoSqlParser::DETERMINISTIC, 0);
}

tree::TerminalNode* PrestoSqlParser::DeterminismContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

size_t PrestoSqlParser::DeterminismContext::getRuleIndex() const {
  return PrestoSqlParser::RuleDeterminism;
}

void PrestoSqlParser::DeterminismContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDeterminism(this);
}

void PrestoSqlParser::DeterminismContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDeterminism(this);
}

antlrcpp::Any PrestoSqlParser::DeterminismContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDeterminism(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::DeterminismContext* PrestoSqlParser::determinism() {
  DeterminismContext* _localctx =
      _tracker.createInstance<DeterminismContext>(_ctx, getState());
  enterRule(_localctx, 40, PrestoSqlParser::RuleDeterminism);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1025);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::DETERMINISTIC: {
        enterOuterAlt(_localctx, 1);
        setState(1022);
        match(PrestoSqlParser::DETERMINISTIC);
        break;
      }

      case PrestoSqlParser::NOT: {
        enterOuterAlt(_localctx, 2);
        setState(1023);
        match(PrestoSqlParser::NOT);
        setState(1024);
        match(PrestoSqlParser::DETERMINISTIC);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NullCallClauseContext
//------------------------------------------------------------------

PrestoSqlParser::NullCallClauseContext::NullCallClauseContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::NullCallClauseContext::RETURNS() {
  return getToken(PrestoSqlParser::RETURNS, 0);
}

std::vector<tree::TerminalNode*>
PrestoSqlParser::NullCallClauseContext::NULL_LITERAL() {
  return getTokens(PrestoSqlParser::NULL_LITERAL);
}

tree::TerminalNode* PrestoSqlParser::NullCallClauseContext::NULL_LITERAL(
    size_t i) {
  return getToken(PrestoSqlParser::NULL_LITERAL, i);
}

tree::TerminalNode* PrestoSqlParser::NullCallClauseContext::ON() {
  return getToken(PrestoSqlParser::ON, 0);
}

tree::TerminalNode* PrestoSqlParser::NullCallClauseContext::INPUT() {
  return getToken(PrestoSqlParser::INPUT, 0);
}

tree::TerminalNode* PrestoSqlParser::NullCallClauseContext::CALLED() {
  return getToken(PrestoSqlParser::CALLED, 0);
}

size_t PrestoSqlParser::NullCallClauseContext::getRuleIndex() const {
  return PrestoSqlParser::RuleNullCallClause;
}

void PrestoSqlParser::NullCallClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNullCallClause(this);
}

void PrestoSqlParser::NullCallClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNullCallClause(this);
}

antlrcpp::Any PrestoSqlParser::NullCallClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNullCallClause(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::NullCallClauseContext* PrestoSqlParser::nullCallClause() {
  NullCallClauseContext* _localctx =
      _tracker.createInstance<NullCallClauseContext>(_ctx, getState());
  enterRule(_localctx, 42, PrestoSqlParser::RuleNullCallClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1036);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::RETURNS: {
        enterOuterAlt(_localctx, 1);
        setState(1027);
        match(PrestoSqlParser::RETURNS);
        setState(1028);
        match(PrestoSqlParser::NULL_LITERAL);
        setState(1029);
        match(PrestoSqlParser::ON);
        setState(1030);
        match(PrestoSqlParser::NULL_LITERAL);
        setState(1031);
        match(PrestoSqlParser::INPUT);
        break;
      }

      case PrestoSqlParser::CALLED: {
        enterOuterAlt(_localctx, 2);
        setState(1032);
        match(PrestoSqlParser::CALLED);
        setState(1033);
        match(PrestoSqlParser::ON);
        setState(1034);
        match(PrestoSqlParser::NULL_LITERAL);
        setState(1035);
        match(PrestoSqlParser::INPUT);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExternalRoutineNameContext
//------------------------------------------------------------------

PrestoSqlParser::ExternalRoutineNameContext::ExternalRoutineNameContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ExternalRoutineNameContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

size_t PrestoSqlParser::ExternalRoutineNameContext::getRuleIndex() const {
  return PrestoSqlParser::RuleExternalRoutineName;
}

void PrestoSqlParser::ExternalRoutineNameContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterExternalRoutineName(this);
}

void PrestoSqlParser::ExternalRoutineNameContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitExternalRoutineName(this);
}

antlrcpp::Any PrestoSqlParser::ExternalRoutineNameContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExternalRoutineName(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ExternalRoutineNameContext*
PrestoSqlParser::externalRoutineName() {
  ExternalRoutineNameContext* _localctx =
      _tracker.createInstance<ExternalRoutineNameContext>(_ctx, getState());
  enterRule(_localctx, 44, PrestoSqlParser::RuleExternalRoutineName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1038);
    identifier();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QueryNoWithContext
//------------------------------------------------------------------

PrestoSqlParser::QueryNoWithContext::QueryNoWithContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::QueryTermContext*
PrestoSqlParser::QueryNoWithContext::queryTerm() {
  return getRuleContext<PrestoSqlParser::QueryTermContext>(0);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::ORDER() {
  return getToken(PrestoSqlParser::ORDER, 0);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::BY() {
  return getToken(PrestoSqlParser::BY, 0);
}

std::vector<PrestoSqlParser::SortItemContext*>
PrestoSqlParser::QueryNoWithContext::sortItem() {
  return getRuleContexts<PrestoSqlParser::SortItemContext>();
}

PrestoSqlParser::SortItemContext* PrestoSqlParser::QueryNoWithContext::sortItem(
    size_t i) {
  return getRuleContext<PrestoSqlParser::SortItemContext>(i);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::OFFSET() {
  return getToken(PrestoSqlParser::OFFSET, 0);
}

std::vector<tree::TerminalNode*>
PrestoSqlParser::QueryNoWithContext::INTEGER_VALUE() {
  return getTokens(PrestoSqlParser::INTEGER_VALUE);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::INTEGER_VALUE(
    size_t i) {
  return getToken(PrestoSqlParser::INTEGER_VALUE, i);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::LIMIT() {
  return getToken(PrestoSqlParser::LIMIT, 0);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::ROW() {
  return getToken(PrestoSqlParser::ROW, 0);
}

std::vector<tree::TerminalNode*> PrestoSqlParser::QueryNoWithContext::ROWS() {
  return getTokens(PrestoSqlParser::ROWS);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::ROWS(size_t i) {
  return getToken(PrestoSqlParser::ROWS, i);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::ALL() {
  return getToken(PrestoSqlParser::ALL, 0);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::FETCH() {
  return getToken(PrestoSqlParser::FETCH, 0);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::FIRST() {
  return getToken(PrestoSqlParser::FIRST, 0);
}

tree::TerminalNode* PrestoSqlParser::QueryNoWithContext::ONLY() {
  return getToken(PrestoSqlParser::ONLY, 0);
}

size_t PrestoSqlParser::QueryNoWithContext::getRuleIndex() const {
  return PrestoSqlParser::RuleQueryNoWith;
}

void PrestoSqlParser::QueryNoWithContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterQueryNoWith(this);
}

void PrestoSqlParser::QueryNoWithContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitQueryNoWith(this);
}

antlrcpp::Any PrestoSqlParser::QueryNoWithContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitQueryNoWith(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::QueryNoWithContext* PrestoSqlParser::queryNoWith() {
  QueryNoWithContext* _localctx =
      _tracker.createInstance<QueryNoWithContext>(_ctx, getState());
  enterRule(_localctx, 46, PrestoSqlParser::RuleQueryNoWith);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1040);
    queryTerm(0);
    setState(1051);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::ORDER) {
      setState(1041);
      match(PrestoSqlParser::ORDER);
      setState(1042);
      match(PrestoSqlParser::BY);
      setState(1043);
      sortItem();
      setState(1048);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PrestoSqlParser::T__3) {
        setState(1044);
        match(PrestoSqlParser::T__3);
        setState(1045);
        sortItem();
        setState(1050);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1058);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::OFFSET) {
      setState(1053);
      match(PrestoSqlParser::OFFSET);
      setState(1054);
      antlrcpp::downCast<QueryNoWithContext*>(_localctx)->offset =
          match(PrestoSqlParser::INTEGER_VALUE);
      setState(1056);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == PrestoSqlParser::ROW

          || _la == PrestoSqlParser::ROWS) {
        setState(1055);
        _la = _input->LA(1);
        if (!(_la == PrestoSqlParser::ROW

              || _la == PrestoSqlParser::ROWS)) {
          _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
      }
    }
    setState(1069);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::FETCH

        || _la == PrestoSqlParser::LIMIT) {
      setState(1067);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case PrestoSqlParser::LIMIT: {
          setState(1060);
          match(PrestoSqlParser::LIMIT);
          setState(1061);
          antlrcpp::downCast<QueryNoWithContext*>(_localctx)->limit =
              _input->LT(1);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::ALL ||
                _la == PrestoSqlParser::INTEGER_VALUE)) {
            antlrcpp::downCast<QueryNoWithContext*>(_localctx)->limit =
                _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
          break;
        }

        case PrestoSqlParser::FETCH: {
          setState(1062);
          match(PrestoSqlParser::FETCH);
          setState(1063);
          match(PrestoSqlParser::FIRST);
          setState(1064);
          antlrcpp::downCast<QueryNoWithContext*>(_localctx)->fetchFirstNRows =
              match(PrestoSqlParser::INTEGER_VALUE);
          setState(1065);
          match(PrestoSqlParser::ROWS);
          setState(1066);
          match(PrestoSqlParser::ONLY);
          break;
        }

        default:
          throw NoViableAltException(this);
      }
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QueryTermContext
//------------------------------------------------------------------

PrestoSqlParser::QueryTermContext::QueryTermContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::QueryTermContext::getRuleIndex() const {
  return PrestoSqlParser::RuleQueryTerm;
}

void PrestoSqlParser::QueryTermContext::copyFrom(QueryTermContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- QueryTermDefaultContext
//------------------------------------------------------------------

PrestoSqlParser::QueryPrimaryContext*
PrestoSqlParser::QueryTermDefaultContext::queryPrimary() {
  return getRuleContext<PrestoSqlParser::QueryPrimaryContext>(0);
}

PrestoSqlParser::QueryTermDefaultContext::QueryTermDefaultContext(
    QueryTermContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::QueryTermDefaultContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterQueryTermDefault(this);
}
void PrestoSqlParser::QueryTermDefaultContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitQueryTermDefault(this);
}

antlrcpp::Any PrestoSqlParser::QueryTermDefaultContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitQueryTermDefault(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SetOperationContext
//------------------------------------------------------------------

std::vector<PrestoSqlParser::QueryTermContext*>
PrestoSqlParser::SetOperationContext::queryTerm() {
  return getRuleContexts<PrestoSqlParser::QueryTermContext>();
}

PrestoSqlParser::QueryTermContext*
PrestoSqlParser::SetOperationContext::queryTerm(size_t i) {
  return getRuleContext<PrestoSqlParser::QueryTermContext>(i);
}

tree::TerminalNode* PrestoSqlParser::SetOperationContext::INTERSECT() {
  return getToken(PrestoSqlParser::INTERSECT, 0);
}

PrestoSqlParser::SetQuantifierContext*
PrestoSqlParser::SetOperationContext::setQuantifier() {
  return getRuleContext<PrestoSqlParser::SetQuantifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::SetOperationContext::UNION() {
  return getToken(PrestoSqlParser::UNION, 0);
}

tree::TerminalNode* PrestoSqlParser::SetOperationContext::EXCEPT() {
  return getToken(PrestoSqlParser::EXCEPT, 0);
}

PrestoSqlParser::SetOperationContext::SetOperationContext(
    QueryTermContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SetOperationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSetOperation(this);
}
void PrestoSqlParser::SetOperationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSetOperation(this);
}

antlrcpp::Any PrestoSqlParser::SetOperationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSetOperation(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::QueryTermContext* PrestoSqlParser::queryTerm() {
  return queryTerm(0);
}

PrestoSqlParser::QueryTermContext* PrestoSqlParser::queryTerm(int precedence) {
  ParserRuleContext* parentContext = _ctx;
  size_t parentState = getState();
  PrestoSqlParser::QueryTermContext* _localctx =
      _tracker.createInstance<QueryTermContext>(_ctx, parentState);
  PrestoSqlParser::QueryTermContext* previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by
                         // generated code.
  size_t startState = 48;
  enterRecursionRule(_localctx, 48, PrestoSqlParser::RuleQueryTerm, precedence);

  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    _localctx = _tracker.createInstance<QueryTermDefaultContext>(_localctx);
    _ctx = _localctx;
    previousContext = _localctx;

    setState(1072);
    queryPrimary();
    _ctx->stop = _input->LT(-1);
    setState(1088);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 129, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1086);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 128, _ctx)) {
          case 1: {
            auto newContext = _tracker.createInstance<SetOperationContext>(
                _tracker.createInstance<QueryTermContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->left = previousContext;
            pushNewRecursionContext(newContext, startState, RuleQueryTerm);
            setState(1074);

            if (!(precpred(_ctx, 2)))
              throw FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(1075);
            antlrcpp::downCast<SetOperationContext*>(_localctx)->op =
                match(PrestoSqlParser::INTERSECT);
            setState(1077);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == PrestoSqlParser::ALL

                || _la == PrestoSqlParser::DISTINCT) {
              setState(1076);
              setQuantifier();
            }
            setState(1079);
            antlrcpp::downCast<SetOperationContext*>(_localctx)->right =
                queryTerm(3);
            break;
          }

          case 2: {
            auto newContext = _tracker.createInstance<SetOperationContext>(
                _tracker.createInstance<QueryTermContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->left = previousContext;
            pushNewRecursionContext(newContext, startState, RuleQueryTerm);
            setState(1080);

            if (!(precpred(_ctx, 1)))
              throw FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(1081);
            antlrcpp::downCast<SetOperationContext*>(_localctx)->op =
                _input->LT(1);
            _la = _input->LA(1);
            if (!(_la == PrestoSqlParser::EXCEPT ||
                  _la == PrestoSqlParser::UNION)) {
              antlrcpp::downCast<SetOperationContext*>(_localctx)->op =
                  _errHandler->recoverInline(this);
            } else {
              _errHandler->reportMatch(this);
              consume();
            }
            setState(1083);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == PrestoSqlParser::ALL

                || _la == PrestoSqlParser::DISTINCT) {
              setState(1082);
              setQuantifier();
            }
            setState(1085);
            antlrcpp::downCast<SetOperationContext*>(_localctx)->right =
                queryTerm(2);
            break;
          }

          default:
            break;
        }
      }
      setState(1090);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 129, _ctx);
    }
  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- QueryPrimaryContext
//------------------------------------------------------------------

PrestoSqlParser::QueryPrimaryContext::QueryPrimaryContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::QueryPrimaryContext::getRuleIndex() const {
  return PrestoSqlParser::RuleQueryPrimary;
}

void PrestoSqlParser::QueryPrimaryContext::copyFrom(QueryPrimaryContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- SubqueryContext
//------------------------------------------------------------------

PrestoSqlParser::QueryNoWithContext*
PrestoSqlParser::SubqueryContext::queryNoWith() {
  return getRuleContext<PrestoSqlParser::QueryNoWithContext>(0);
}

PrestoSqlParser::SubqueryContext::SubqueryContext(QueryPrimaryContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SubqueryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSubquery(this);
}
void PrestoSqlParser::SubqueryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSubquery(this);
}

antlrcpp::Any PrestoSqlParser::SubqueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSubquery(this);
  else
    return visitor->visitChildren(this);
}
//----------------- QueryPrimaryDefaultContext
//------------------------------------------------------------------

PrestoSqlParser::QuerySpecificationContext*
PrestoSqlParser::QueryPrimaryDefaultContext::querySpecification() {
  return getRuleContext<PrestoSqlParser::QuerySpecificationContext>(0);
}

PrestoSqlParser::QueryPrimaryDefaultContext::QueryPrimaryDefaultContext(
    QueryPrimaryContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::QueryPrimaryDefaultContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterQueryPrimaryDefault(this);
}
void PrestoSqlParser::QueryPrimaryDefaultContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitQueryPrimaryDefault(this);
}

antlrcpp::Any PrestoSqlParser::QueryPrimaryDefaultContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitQueryPrimaryDefault(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::TableContext::TABLE() {
  return getToken(PrestoSqlParser::TABLE, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::TableContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::TableContext::TableContext(QueryPrimaryContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::TableContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTable(this);
}
void PrestoSqlParser::TableContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTable(this);
}

antlrcpp::Any PrestoSqlParser::TableContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTable(this);
  else
    return visitor->visitChildren(this);
}
//----------------- InlineTableContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::InlineTableContext::VALUES() {
  return getToken(PrestoSqlParser::VALUES, 0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::InlineTableContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::InlineTableContext::expression(size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

PrestoSqlParser::InlineTableContext::InlineTableContext(
    QueryPrimaryContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::InlineTableContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterInlineTable(this);
}
void PrestoSqlParser::InlineTableContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitInlineTable(this);
}

antlrcpp::Any PrestoSqlParser::InlineTableContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitInlineTable(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::QueryPrimaryContext* PrestoSqlParser::queryPrimary() {
  QueryPrimaryContext* _localctx =
      _tracker.createInstance<QueryPrimaryContext>(_ctx, getState());
  enterRule(_localctx, 50, PrestoSqlParser::RuleQueryPrimary);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(1107);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::SELECT: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::QueryPrimaryDefaultContext>(
                    _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1091);
        querySpecification();
        break;
      }

      case PrestoSqlParser::TABLE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TableContext>(_localctx);
        enterOuterAlt(_localctx, 2);
        setState(1092);
        match(PrestoSqlParser::TABLE);
        setState(1093);
        qualifiedName();
        break;
      }

      case PrestoSqlParser::VALUES: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::InlineTableContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(1094);
        match(PrestoSqlParser::VALUES);
        setState(1095);
        expression();
        setState(1100);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 130, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(1096);
            match(PrestoSqlParser::T__3);
            setState(1097);
            expression();
          }
          setState(1102);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 130, _ctx);
        }
        break;
      }

      case PrestoSqlParser::T__1: {
        _localctx = _tracker.createInstance<PrestoSqlParser::SubqueryContext>(
            _localctx);
        enterOuterAlt(_localctx, 4);
        setState(1103);
        match(PrestoSqlParser::T__1);
        setState(1104);
        queryNoWith();
        setState(1105);
        match(PrestoSqlParser::T__2);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SortItemContext
//------------------------------------------------------------------

PrestoSqlParser::SortItemContext::SortItemContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::SortItemContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::SortItemContext::NULLS() {
  return getToken(PrestoSqlParser::NULLS, 0);
}

tree::TerminalNode* PrestoSqlParser::SortItemContext::ASC() {
  return getToken(PrestoSqlParser::ASC, 0);
}

tree::TerminalNode* PrestoSqlParser::SortItemContext::DESC() {
  return getToken(PrestoSqlParser::DESC, 0);
}

tree::TerminalNode* PrestoSqlParser::SortItemContext::FIRST() {
  return getToken(PrestoSqlParser::FIRST, 0);
}

tree::TerminalNode* PrestoSqlParser::SortItemContext::LAST() {
  return getToken(PrestoSqlParser::LAST, 0);
}

size_t PrestoSqlParser::SortItemContext::getRuleIndex() const {
  return PrestoSqlParser::RuleSortItem;
}

void PrestoSqlParser::SortItemContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSortItem(this);
}

void PrestoSqlParser::SortItemContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSortItem(this);
}

antlrcpp::Any PrestoSqlParser::SortItemContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSortItem(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::SortItemContext* PrestoSqlParser::sortItem() {
  SortItemContext* _localctx =
      _tracker.createInstance<SortItemContext>(_ctx, getState());
  enterRule(_localctx, 52, PrestoSqlParser::RuleSortItem);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1109);
    expression();
    setState(1111);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::ASC

        || _la == PrestoSqlParser::DESC) {
      setState(1110);
      antlrcpp::downCast<SortItemContext*>(_localctx)->ordering = _input->LT(1);
      _la = _input->LA(1);
      if (!(_la == PrestoSqlParser::ASC

            || _la == PrestoSqlParser::DESC)) {
        antlrcpp::downCast<SortItemContext*>(_localctx)->ordering =
            _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(1115);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::NULLS) {
      setState(1113);
      match(PrestoSqlParser::NULLS);
      setState(1114);
      antlrcpp::downCast<SortItemContext*>(_localctx)->nullOrdering =
          _input->LT(1);
      _la = _input->LA(1);
      if (!(_la == PrestoSqlParser::FIRST

            || _la == PrestoSqlParser::LAST)) {
        antlrcpp::downCast<SortItemContext*>(_localctx)->nullOrdering =
            _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QuerySpecificationContext
//------------------------------------------------------------------

PrestoSqlParser::QuerySpecificationContext::QuerySpecificationContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::QuerySpecificationContext::SELECT() {
  return getToken(PrestoSqlParser::SELECT, 0);
}

std::vector<PrestoSqlParser::SelectItemContext*>
PrestoSqlParser::QuerySpecificationContext::selectItem() {
  return getRuleContexts<PrestoSqlParser::SelectItemContext>();
}

PrestoSqlParser::SelectItemContext*
PrestoSqlParser::QuerySpecificationContext::selectItem(size_t i) {
  return getRuleContext<PrestoSqlParser::SelectItemContext>(i);
}

PrestoSqlParser::SetQuantifierContext*
PrestoSqlParser::QuerySpecificationContext::setQuantifier() {
  return getRuleContext<PrestoSqlParser::SetQuantifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::QuerySpecificationContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

std::vector<PrestoSqlParser::RelationContext*>
PrestoSqlParser::QuerySpecificationContext::relation() {
  return getRuleContexts<PrestoSqlParser::RelationContext>();
}

PrestoSqlParser::RelationContext*
PrestoSqlParser::QuerySpecificationContext::relation(size_t i) {
  return getRuleContext<PrestoSqlParser::RelationContext>(i);
}

tree::TerminalNode* PrestoSqlParser::QuerySpecificationContext::WHERE() {
  return getToken(PrestoSqlParser::WHERE, 0);
}

tree::TerminalNode* PrestoSqlParser::QuerySpecificationContext::GROUP() {
  return getToken(PrestoSqlParser::GROUP, 0);
}

tree::TerminalNode* PrestoSqlParser::QuerySpecificationContext::BY() {
  return getToken(PrestoSqlParser::BY, 0);
}

PrestoSqlParser::GroupByContext*
PrestoSqlParser::QuerySpecificationContext::groupBy() {
  return getRuleContext<PrestoSqlParser::GroupByContext>(0);
}

tree::TerminalNode* PrestoSqlParser::QuerySpecificationContext::HAVING() {
  return getToken(PrestoSqlParser::HAVING, 0);
}

std::vector<PrestoSqlParser::BooleanExpressionContext*>
PrestoSqlParser::QuerySpecificationContext::booleanExpression() {
  return getRuleContexts<PrestoSqlParser::BooleanExpressionContext>();
}

PrestoSqlParser::BooleanExpressionContext*
PrestoSqlParser::QuerySpecificationContext::booleanExpression(size_t i) {
  return getRuleContext<PrestoSqlParser::BooleanExpressionContext>(i);
}

size_t PrestoSqlParser::QuerySpecificationContext::getRuleIndex() const {
  return PrestoSqlParser::RuleQuerySpecification;
}

void PrestoSqlParser::QuerySpecificationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuerySpecification(this);
}

void PrestoSqlParser::QuerySpecificationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuerySpecification(this);
}

antlrcpp::Any PrestoSqlParser::QuerySpecificationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitQuerySpecification(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::QuerySpecificationContext*
PrestoSqlParser::querySpecification() {
  QuerySpecificationContext* _localctx =
      _tracker.createInstance<QuerySpecificationContext>(_ctx, getState());
  enterRule(_localctx, 54, PrestoSqlParser::RuleQuerySpecification);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1117);
    match(PrestoSqlParser::SELECT);
    setState(1119);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 134, _ctx)) {
      case 1: {
        setState(1118);
        setQuantifier();
        break;
      }

      default:
        break;
    }
    setState(1121);
    selectItem();
    setState(1126);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 135, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1122);
        match(PrestoSqlParser::T__3);
        setState(1123);
        selectItem();
      }
      setState(1128);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 135, _ctx);
    }
    setState(1138);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 137, _ctx)) {
      case 1: {
        setState(1129);
        match(PrestoSqlParser::FROM);
        setState(1130);
        relation(0);
        setState(1135);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 136, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(1131);
            match(PrestoSqlParser::T__3);
            setState(1132);
            relation(0);
          }
          setState(1137);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 136, _ctx);
        }
        break;
      }

      default:
        break;
    }
    setState(1142);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 138, _ctx)) {
      case 1: {
        setState(1140);
        match(PrestoSqlParser::WHERE);
        setState(1141);
        antlrcpp::downCast<QuerySpecificationContext*>(_localctx)->where =
            booleanExpression(0);
        break;
      }

      default:
        break;
    }
    setState(1147);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 139, _ctx)) {
      case 1: {
        setState(1144);
        match(PrestoSqlParser::GROUP);
        setState(1145);
        match(PrestoSqlParser::BY);
        setState(1146);
        groupBy();
        break;
      }

      default:
        break;
    }
    setState(1151);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 140, _ctx)) {
      case 1: {
        setState(1149);
        match(PrestoSqlParser::HAVING);
        setState(1150);
        antlrcpp::downCast<QuerySpecificationContext*>(_localctx)->having =
            booleanExpression(0);
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupByContext
//------------------------------------------------------------------

PrestoSqlParser::GroupByContext::GroupByContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::GroupingElementContext*>
PrestoSqlParser::GroupByContext::groupingElement() {
  return getRuleContexts<PrestoSqlParser::GroupingElementContext>();
}

PrestoSqlParser::GroupingElementContext*
PrestoSqlParser::GroupByContext::groupingElement(size_t i) {
  return getRuleContext<PrestoSqlParser::GroupingElementContext>(i);
}

PrestoSqlParser::SetQuantifierContext*
PrestoSqlParser::GroupByContext::setQuantifier() {
  return getRuleContext<PrestoSqlParser::SetQuantifierContext>(0);
}

size_t PrestoSqlParser::GroupByContext::getRuleIndex() const {
  return PrestoSqlParser::RuleGroupBy;
}

void PrestoSqlParser::GroupByContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupBy(this);
}

void PrestoSqlParser::GroupByContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupBy(this);
}

antlrcpp::Any PrestoSqlParser::GroupByContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitGroupBy(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::GroupByContext* PrestoSqlParser::groupBy() {
  GroupByContext* _localctx =
      _tracker.createInstance<GroupByContext>(_ctx, getState());
  enterRule(_localctx, 56, PrestoSqlParser::RuleGroupBy);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1154);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 141, _ctx)) {
      case 1: {
        setState(1153);
        setQuantifier();
        break;
      }

      default:
        break;
    }
    setState(1156);
    groupingElement();
    setState(1161);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 142, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1157);
        match(PrestoSqlParser::T__3);
        setState(1158);
        groupingElement();
      }
      setState(1163);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 142, _ctx);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupingElementContext
//------------------------------------------------------------------

PrestoSqlParser::GroupingElementContext::GroupingElementContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::GroupingElementContext::getRuleIndex() const {
  return PrestoSqlParser::RuleGroupingElement;
}

void PrestoSqlParser::GroupingElementContext::copyFrom(
    GroupingElementContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- MultipleGroupingSetsContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::MultipleGroupingSetsContext::GROUPING() {
  return getToken(PrestoSqlParser::GROUPING, 0);
}

tree::TerminalNode* PrestoSqlParser::MultipleGroupingSetsContext::SETS() {
  return getToken(PrestoSqlParser::SETS, 0);
}

std::vector<PrestoSqlParser::GroupingSetContext*>
PrestoSqlParser::MultipleGroupingSetsContext::groupingSet() {
  return getRuleContexts<PrestoSqlParser::GroupingSetContext>();
}

PrestoSqlParser::GroupingSetContext*
PrestoSqlParser::MultipleGroupingSetsContext::groupingSet(size_t i) {
  return getRuleContext<PrestoSqlParser::GroupingSetContext>(i);
}

PrestoSqlParser::MultipleGroupingSetsContext::MultipleGroupingSetsContext(
    GroupingElementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::MultipleGroupingSetsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterMultipleGroupingSets(this);
}
void PrestoSqlParser::MultipleGroupingSetsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitMultipleGroupingSets(this);
}

antlrcpp::Any PrestoSqlParser::MultipleGroupingSetsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitMultipleGroupingSets(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SingleGroupingSetContext
//------------------------------------------------------------------

PrestoSqlParser::GroupingSetContext*
PrestoSqlParser::SingleGroupingSetContext::groupingSet() {
  return getRuleContext<PrestoSqlParser::GroupingSetContext>(0);
}

PrestoSqlParser::SingleGroupingSetContext::SingleGroupingSetContext(
    GroupingElementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SingleGroupingSetContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSingleGroupingSet(this);
}
void PrestoSqlParser::SingleGroupingSetContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSingleGroupingSet(this);
}

antlrcpp::Any PrestoSqlParser::SingleGroupingSetContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSingleGroupingSet(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CubeContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CubeContext::CUBE() {
  return getToken(PrestoSqlParser::CUBE, 0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::CubeContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext* PrestoSqlParser::CubeContext::expression(
    size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

PrestoSqlParser::CubeContext::CubeContext(GroupingElementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CubeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCube(this);
}
void PrestoSqlParser::CubeContext::exitRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCube(this);
}

antlrcpp::Any PrestoSqlParser::CubeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCube(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RollupContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RollupContext::ROLLUP() {
  return getToken(PrestoSqlParser::ROLLUP, 0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::RollupContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext* PrestoSqlParser::RollupContext::expression(
    size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

PrestoSqlParser::RollupContext::RollupContext(GroupingElementContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RollupContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRollup(this);
}
void PrestoSqlParser::RollupContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRollup(this);
}

antlrcpp::Any PrestoSqlParser::RollupContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRollup(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::GroupingElementContext* PrestoSqlParser::groupingElement() {
  GroupingElementContext* _localctx =
      _tracker.createInstance<GroupingElementContext>(_ctx, getState());
  enterRule(_localctx, 58, PrestoSqlParser::RuleGroupingElement);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1204);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 148, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SingleGroupingSetContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1164);
        groupingSet();
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RollupContext>(_localctx);
        enterOuterAlt(_localctx, 2);
        setState(1165);
        match(PrestoSqlParser::ROLLUP);
        setState(1166);
        match(PrestoSqlParser::T__1);
        setState(1175);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) &
              ((1ULL << PrestoSqlParser::T__1) |
               (1ULL << PrestoSqlParser::T__4) |
               (1ULL << PrestoSqlParser::ADD) |
               (1ULL << PrestoSqlParser::ADMIN) |
               (1ULL << PrestoSqlParser::ALL) |
               (1ULL << PrestoSqlParser::ANALYZE) |
               (1ULL << PrestoSqlParser::ANY) |
               (1ULL << PrestoSqlParser::ARRAY) |
               (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
               (1ULL << PrestoSqlParser::BEFORE) |
               (1ULL << PrestoSqlParser::BERNOULLI) |
               (1ULL << PrestoSqlParser::CALL) |
               (1ULL << PrestoSqlParser::CALLED) |
               (1ULL << PrestoSqlParser::CASCADE) |
               (1ULL << PrestoSqlParser::CASE) |
               (1ULL << PrestoSqlParser::CAST) |
               (1ULL << PrestoSqlParser::CATALOGS) |
               (1ULL << PrestoSqlParser::COLUMN) |
               (1ULL << PrestoSqlParser::COLUMNS) |
               (1ULL << PrestoSqlParser::COMMENT) |
               (1ULL << PrestoSqlParser::COMMIT) |
               (1ULL << PrestoSqlParser::COMMITTED) |
               (1ULL << PrestoSqlParser::CURRENT) |
               (1ULL << PrestoSqlParser::CURRENT_DATE) |
               (1ULL << PrestoSqlParser::CURRENT_ROLE) |
               (1ULL << PrestoSqlParser::CURRENT_TIME) |
               (1ULL << PrestoSqlParser::CURRENT_TIMESTAMP) |
               (1ULL << PrestoSqlParser::CURRENT_USER) |
               (1ULL << PrestoSqlParser::DATA) |
               (1ULL << PrestoSqlParser::DATE) |
               (1ULL << PrestoSqlParser::DAY) |
               (1ULL << PrestoSqlParser::DEFINER) |
               (1ULL << PrestoSqlParser::DESC) |
               (1ULL << PrestoSqlParser::DETERMINISTIC) |
               (1ULL << PrestoSqlParser::DISABLED) |
               (1ULL << PrestoSqlParser::DISTRIBUTED) |
               (1ULL << PrestoSqlParser::ENABLED) |
               (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) &
              ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::EXISTS - 66)) |
               (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
               (1ULL << (PrestoSqlParser::EXTRACT - 66)) |
               (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
               (1ULL << (PrestoSqlParser::FALSE - 66)) |
               (1ULL << (PrestoSqlParser::FETCH - 66)) |
               (1ULL << (PrestoSqlParser::FILTER - 66)) |
               (1ULL << (PrestoSqlParser::FIRST - 66)) |
               (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
               (1ULL << (PrestoSqlParser::FORMAT - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
               (1ULL << (PrestoSqlParser::GRANT - 66)) |
               (1ULL << (PrestoSqlParser::GRANTED - 66)) |
               (1ULL << (PrestoSqlParser::GRANTS - 66)) |
               (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
               (1ULL << (PrestoSqlParser::GROUPING - 66)) |
               (1ULL << (PrestoSqlParser::GROUPS - 66)) |
               (1ULL << (PrestoSqlParser::HOUR - 66)) |
               (1ULL << (PrestoSqlParser::IF - 66)) |
               (1ULL << (PrestoSqlParser::IGNORE - 66)) |
               (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::INPUT - 66)) |
               (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
               (1ULL << (PrestoSqlParser::INVOKER - 66)) |
               (1ULL << (PrestoSqlParser::IO - 66)) |
               (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
               (1ULL << (PrestoSqlParser::JSON - 66)) |
               (1ULL << (PrestoSqlParser::KEY - 66)) |
               (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
               (1ULL << (PrestoSqlParser::LAST - 66)) |
               (1ULL << (PrestoSqlParser::LATERAL - 66)) |
               (1ULL << (PrestoSqlParser::LEVEL - 66)) |
               (1ULL << (PrestoSqlParser::LIMIT - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIME - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIMESTAMP - 66)) |
               (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
               (1ULL << (PrestoSqlParser::MAP - 66)) |
               (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
               (1ULL << (PrestoSqlParser::MINUTE - 66)) |
               (1ULL << (PrestoSqlParser::MONTH - 66)) |
               (1ULL << (PrestoSqlParser::NAME - 66)) |
               (1ULL << (PrestoSqlParser::NFC - 66)) |
               (1ULL << (PrestoSqlParser::NFD - 66)) |
               (1ULL << (PrestoSqlParser::NFKC - 66)) |
               (1ULL << (PrestoSqlParser::NFKD - 66)) |
               (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) &
              ((1ULL << (PrestoSqlParser::NONE - 130)) |
               (1ULL << (PrestoSqlParser::NORMALIZE - 130)) |
               (1ULL << (PrestoSqlParser::NOT - 130)) |
               (1ULL << (PrestoSqlParser::NULL_LITERAL - 130)) |
               (1ULL << (PrestoSqlParser::NULLIF - 130)) |
               (1ULL << (PrestoSqlParser::NULLS - 130)) |
               (1ULL << (PrestoSqlParser::OF - 130)) |
               (1ULL << (PrestoSqlParser::OFFSET - 130)) |
               (1ULL << (PrestoSqlParser::ONLY - 130)) |
               (1ULL << (PrestoSqlParser::OPTION - 130)) |
               (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
               (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
               (1ULL << (PrestoSqlParser::OVER - 130)) |
               (1ULL << (PrestoSqlParser::PARTITION - 130)) |
               (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
               (1ULL << (PrestoSqlParser::POSITION - 130)) |
               (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
               (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
               (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
               (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
               (1ULL << (PrestoSqlParser::RANGE - 130)) |
               (1ULL << (PrestoSqlParser::READ - 130)) |
               (1ULL << (PrestoSqlParser::REFRESH - 130)) |
               (1ULL << (PrestoSqlParser::RELY - 130)) |
               (1ULL << (PrestoSqlParser::RENAME - 130)) |
               (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
               (1ULL << (PrestoSqlParser::REPLACE - 130)) |
               (1ULL << (PrestoSqlParser::RESET - 130)) |
               (1ULL << (PrestoSqlParser::RESPECT - 130)) |
               (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
               (1ULL << (PrestoSqlParser::RETURN - 130)) |
               (1ULL << (PrestoSqlParser::RETURNS - 130)) |
               (1ULL << (PrestoSqlParser::REVOKE - 130)) |
               (1ULL << (PrestoSqlParser::ROLE - 130)) |
               (1ULL << (PrestoSqlParser::ROLES - 130)) |
               (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
               (1ULL << (PrestoSqlParser::ROW - 130)) |
               (1ULL << (PrestoSqlParser::ROWS - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
               (1ULL << (PrestoSqlParser::SECOND - 130)) |
               (1ULL << (PrestoSqlParser::SECURITY - 130)) |
               (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
               (1ULL << (PrestoSqlParser::SESSION - 130)) |
               (1ULL << (PrestoSqlParser::SET - 130)) |
               (1ULL << (PrestoSqlParser::SETS - 130)) |
               (1ULL << (PrestoSqlParser::SHOW - 130)) |
               (1ULL << (PrestoSqlParser::SOME - 130)) |
               (1ULL << (PrestoSqlParser::SQL - 130)) |
               (1ULL << (PrestoSqlParser::START - 130)) |
               (1ULL << (PrestoSqlParser::STATS - 130)) |
               (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
            ((((_la - 195) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 195)) &
              ((1ULL << (PrestoSqlParser::TABLES - 195)) |
               (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
               (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
               (1ULL << (PrestoSqlParser::TEXT - 195)) |
               (1ULL << (PrestoSqlParser::TIME - 195)) |
               (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
               (1ULL << (PrestoSqlParser::TO - 195)) |
               (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
               (1ULL << (PrestoSqlParser::TRUE - 195)) |
               (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
               (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
               (1ULL << (PrestoSqlParser::TYPE - 195)) |
               (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
               (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
               (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
               (1ULL << (PrestoSqlParser::UPDATE - 195)) |
               (1ULL << (PrestoSqlParser::USE - 195)) |
               (1ULL << (PrestoSqlParser::USER - 195)) |
               (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
               (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
               (1ULL << (PrestoSqlParser::VERSION - 195)) |
               (1ULL << (PrestoSqlParser::VIEW - 195)) |
               (1ULL << (PrestoSqlParser::WORK - 195)) |
               (1ULL << (PrestoSqlParser::WRITE - 195)) |
               (1ULL << (PrestoSqlParser::YEAR - 195)) |
               (1ULL << (PrestoSqlParser::ZONE - 195)) |
               (1ULL << (PrestoSqlParser::PLUS - 195)) |
               (1ULL << (PrestoSqlParser::MINUS - 195)) |
               (1ULL << (PrestoSqlParser::STRING - 195)) |
               (1ULL << (PrestoSqlParser::UNICODE_STRING - 195)) |
               (1ULL << (PrestoSqlParser::BINARY_LITERAL - 195)) |
               (1ULL << (PrestoSqlParser::INTEGER_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DECIMAL_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DIGIT_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::QUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::BACKQUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_PRECISION - 195)))) != 0)) {
          setState(1167);
          expression();
          setState(1172);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1168);
            match(PrestoSqlParser::T__3);
            setState(1169);
            expression();
            setState(1174);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1177);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CubeContext>(_localctx);
        enterOuterAlt(_localctx, 3);
        setState(1178);
        match(PrestoSqlParser::CUBE);
        setState(1179);
        match(PrestoSqlParser::T__1);
        setState(1188);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) &
              ((1ULL << PrestoSqlParser::T__1) |
               (1ULL << PrestoSqlParser::T__4) |
               (1ULL << PrestoSqlParser::ADD) |
               (1ULL << PrestoSqlParser::ADMIN) |
               (1ULL << PrestoSqlParser::ALL) |
               (1ULL << PrestoSqlParser::ANALYZE) |
               (1ULL << PrestoSqlParser::ANY) |
               (1ULL << PrestoSqlParser::ARRAY) |
               (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
               (1ULL << PrestoSqlParser::BEFORE) |
               (1ULL << PrestoSqlParser::BERNOULLI) |
               (1ULL << PrestoSqlParser::CALL) |
               (1ULL << PrestoSqlParser::CALLED) |
               (1ULL << PrestoSqlParser::CASCADE) |
               (1ULL << PrestoSqlParser::CASE) |
               (1ULL << PrestoSqlParser::CAST) |
               (1ULL << PrestoSqlParser::CATALOGS) |
               (1ULL << PrestoSqlParser::COLUMN) |
               (1ULL << PrestoSqlParser::COLUMNS) |
               (1ULL << PrestoSqlParser::COMMENT) |
               (1ULL << PrestoSqlParser::COMMIT) |
               (1ULL << PrestoSqlParser::COMMITTED) |
               (1ULL << PrestoSqlParser::CURRENT) |
               (1ULL << PrestoSqlParser::CURRENT_DATE) |
               (1ULL << PrestoSqlParser::CURRENT_ROLE) |
               (1ULL << PrestoSqlParser::CURRENT_TIME) |
               (1ULL << PrestoSqlParser::CURRENT_TIMESTAMP) |
               (1ULL << PrestoSqlParser::CURRENT_USER) |
               (1ULL << PrestoSqlParser::DATA) |
               (1ULL << PrestoSqlParser::DATE) |
               (1ULL << PrestoSqlParser::DAY) |
               (1ULL << PrestoSqlParser::DEFINER) |
               (1ULL << PrestoSqlParser::DESC) |
               (1ULL << PrestoSqlParser::DETERMINISTIC) |
               (1ULL << PrestoSqlParser::DISABLED) |
               (1ULL << PrestoSqlParser::DISTRIBUTED) |
               (1ULL << PrestoSqlParser::ENABLED) |
               (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) &
              ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::EXISTS - 66)) |
               (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
               (1ULL << (PrestoSqlParser::EXTRACT - 66)) |
               (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
               (1ULL << (PrestoSqlParser::FALSE - 66)) |
               (1ULL << (PrestoSqlParser::FETCH - 66)) |
               (1ULL << (PrestoSqlParser::FILTER - 66)) |
               (1ULL << (PrestoSqlParser::FIRST - 66)) |
               (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
               (1ULL << (PrestoSqlParser::FORMAT - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
               (1ULL << (PrestoSqlParser::GRANT - 66)) |
               (1ULL << (PrestoSqlParser::GRANTED - 66)) |
               (1ULL << (PrestoSqlParser::GRANTS - 66)) |
               (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
               (1ULL << (PrestoSqlParser::GROUPING - 66)) |
               (1ULL << (PrestoSqlParser::GROUPS - 66)) |
               (1ULL << (PrestoSqlParser::HOUR - 66)) |
               (1ULL << (PrestoSqlParser::IF - 66)) |
               (1ULL << (PrestoSqlParser::IGNORE - 66)) |
               (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::INPUT - 66)) |
               (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
               (1ULL << (PrestoSqlParser::INVOKER - 66)) |
               (1ULL << (PrestoSqlParser::IO - 66)) |
               (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
               (1ULL << (PrestoSqlParser::JSON - 66)) |
               (1ULL << (PrestoSqlParser::KEY - 66)) |
               (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
               (1ULL << (PrestoSqlParser::LAST - 66)) |
               (1ULL << (PrestoSqlParser::LATERAL - 66)) |
               (1ULL << (PrestoSqlParser::LEVEL - 66)) |
               (1ULL << (PrestoSqlParser::LIMIT - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIME - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIMESTAMP - 66)) |
               (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
               (1ULL << (PrestoSqlParser::MAP - 66)) |
               (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
               (1ULL << (PrestoSqlParser::MINUTE - 66)) |
               (1ULL << (PrestoSqlParser::MONTH - 66)) |
               (1ULL << (PrestoSqlParser::NAME - 66)) |
               (1ULL << (PrestoSqlParser::NFC - 66)) |
               (1ULL << (PrestoSqlParser::NFD - 66)) |
               (1ULL << (PrestoSqlParser::NFKC - 66)) |
               (1ULL << (PrestoSqlParser::NFKD - 66)) |
               (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) &
              ((1ULL << (PrestoSqlParser::NONE - 130)) |
               (1ULL << (PrestoSqlParser::NORMALIZE - 130)) |
               (1ULL << (PrestoSqlParser::NOT - 130)) |
               (1ULL << (PrestoSqlParser::NULL_LITERAL - 130)) |
               (1ULL << (PrestoSqlParser::NULLIF - 130)) |
               (1ULL << (PrestoSqlParser::NULLS - 130)) |
               (1ULL << (PrestoSqlParser::OF - 130)) |
               (1ULL << (PrestoSqlParser::OFFSET - 130)) |
               (1ULL << (PrestoSqlParser::ONLY - 130)) |
               (1ULL << (PrestoSqlParser::OPTION - 130)) |
               (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
               (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
               (1ULL << (PrestoSqlParser::OVER - 130)) |
               (1ULL << (PrestoSqlParser::PARTITION - 130)) |
               (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
               (1ULL << (PrestoSqlParser::POSITION - 130)) |
               (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
               (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
               (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
               (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
               (1ULL << (PrestoSqlParser::RANGE - 130)) |
               (1ULL << (PrestoSqlParser::READ - 130)) |
               (1ULL << (PrestoSqlParser::REFRESH - 130)) |
               (1ULL << (PrestoSqlParser::RELY - 130)) |
               (1ULL << (PrestoSqlParser::RENAME - 130)) |
               (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
               (1ULL << (PrestoSqlParser::REPLACE - 130)) |
               (1ULL << (PrestoSqlParser::RESET - 130)) |
               (1ULL << (PrestoSqlParser::RESPECT - 130)) |
               (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
               (1ULL << (PrestoSqlParser::RETURN - 130)) |
               (1ULL << (PrestoSqlParser::RETURNS - 130)) |
               (1ULL << (PrestoSqlParser::REVOKE - 130)) |
               (1ULL << (PrestoSqlParser::ROLE - 130)) |
               (1ULL << (PrestoSqlParser::ROLES - 130)) |
               (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
               (1ULL << (PrestoSqlParser::ROW - 130)) |
               (1ULL << (PrestoSqlParser::ROWS - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
               (1ULL << (PrestoSqlParser::SECOND - 130)) |
               (1ULL << (PrestoSqlParser::SECURITY - 130)) |
               (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
               (1ULL << (PrestoSqlParser::SESSION - 130)) |
               (1ULL << (PrestoSqlParser::SET - 130)) |
               (1ULL << (PrestoSqlParser::SETS - 130)) |
               (1ULL << (PrestoSqlParser::SHOW - 130)) |
               (1ULL << (PrestoSqlParser::SOME - 130)) |
               (1ULL << (PrestoSqlParser::SQL - 130)) |
               (1ULL << (PrestoSqlParser::START - 130)) |
               (1ULL << (PrestoSqlParser::STATS - 130)) |
               (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
            ((((_la - 195) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 195)) &
              ((1ULL << (PrestoSqlParser::TABLES - 195)) |
               (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
               (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
               (1ULL << (PrestoSqlParser::TEXT - 195)) |
               (1ULL << (PrestoSqlParser::TIME - 195)) |
               (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
               (1ULL << (PrestoSqlParser::TO - 195)) |
               (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
               (1ULL << (PrestoSqlParser::TRUE - 195)) |
               (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
               (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
               (1ULL << (PrestoSqlParser::TYPE - 195)) |
               (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
               (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
               (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
               (1ULL << (PrestoSqlParser::UPDATE - 195)) |
               (1ULL << (PrestoSqlParser::USE - 195)) |
               (1ULL << (PrestoSqlParser::USER - 195)) |
               (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
               (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
               (1ULL << (PrestoSqlParser::VERSION - 195)) |
               (1ULL << (PrestoSqlParser::VIEW - 195)) |
               (1ULL << (PrestoSqlParser::WORK - 195)) |
               (1ULL << (PrestoSqlParser::WRITE - 195)) |
               (1ULL << (PrestoSqlParser::YEAR - 195)) |
               (1ULL << (PrestoSqlParser::ZONE - 195)) |
               (1ULL << (PrestoSqlParser::PLUS - 195)) |
               (1ULL << (PrestoSqlParser::MINUS - 195)) |
               (1ULL << (PrestoSqlParser::STRING - 195)) |
               (1ULL << (PrestoSqlParser::UNICODE_STRING - 195)) |
               (1ULL << (PrestoSqlParser::BINARY_LITERAL - 195)) |
               (1ULL << (PrestoSqlParser::INTEGER_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DECIMAL_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DIGIT_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::QUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::BACKQUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_PRECISION - 195)))) != 0)) {
          setState(1180);
          expression();
          setState(1185);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1181);
            match(PrestoSqlParser::T__3);
            setState(1182);
            expression();
            setState(1187);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1190);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 4: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::MultipleGroupingSetsContext>(
                    _localctx);
        enterOuterAlt(_localctx, 4);
        setState(1191);
        match(PrestoSqlParser::GROUPING);
        setState(1192);
        match(PrestoSqlParser::SETS);
        setState(1193);
        match(PrestoSqlParser::T__1);
        setState(1194);
        groupingSet();
        setState(1199);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1195);
          match(PrestoSqlParser::T__3);
          setState(1196);
          groupingSet();
          setState(1201);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1202);
        match(PrestoSqlParser::T__2);
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupingSetContext
//------------------------------------------------------------------

PrestoSqlParser::GroupingSetContext::GroupingSetContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::GroupingSetContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::GroupingSetContext::expression(size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

size_t PrestoSqlParser::GroupingSetContext::getRuleIndex() const {
  return PrestoSqlParser::RuleGroupingSet;
}

void PrestoSqlParser::GroupingSetContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupingSet(this);
}

void PrestoSqlParser::GroupingSetContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupingSet(this);
}

antlrcpp::Any PrestoSqlParser::GroupingSetContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitGroupingSet(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::GroupingSetContext* PrestoSqlParser::groupingSet() {
  GroupingSetContext* _localctx =
      _tracker.createInstance<GroupingSetContext>(_ctx, getState());
  enterRule(_localctx, 60, PrestoSqlParser::RuleGroupingSet);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1219);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 151, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(1206);
        match(PrestoSqlParser::T__1);
        setState(1215);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) &
              ((1ULL << PrestoSqlParser::T__1) |
               (1ULL << PrestoSqlParser::T__4) |
               (1ULL << PrestoSqlParser::ADD) |
               (1ULL << PrestoSqlParser::ADMIN) |
               (1ULL << PrestoSqlParser::ALL) |
               (1ULL << PrestoSqlParser::ANALYZE) |
               (1ULL << PrestoSqlParser::ANY) |
               (1ULL << PrestoSqlParser::ARRAY) |
               (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
               (1ULL << PrestoSqlParser::BEFORE) |
               (1ULL << PrestoSqlParser::BERNOULLI) |
               (1ULL << PrestoSqlParser::CALL) |
               (1ULL << PrestoSqlParser::CALLED) |
               (1ULL << PrestoSqlParser::CASCADE) |
               (1ULL << PrestoSqlParser::CASE) |
               (1ULL << PrestoSqlParser::CAST) |
               (1ULL << PrestoSqlParser::CATALOGS) |
               (1ULL << PrestoSqlParser::COLUMN) |
               (1ULL << PrestoSqlParser::COLUMNS) |
               (1ULL << PrestoSqlParser::COMMENT) |
               (1ULL << PrestoSqlParser::COMMIT) |
               (1ULL << PrestoSqlParser::COMMITTED) |
               (1ULL << PrestoSqlParser::CURRENT) |
               (1ULL << PrestoSqlParser::CURRENT_DATE) |
               (1ULL << PrestoSqlParser::CURRENT_ROLE) |
               (1ULL << PrestoSqlParser::CURRENT_TIME) |
               (1ULL << PrestoSqlParser::CURRENT_TIMESTAMP) |
               (1ULL << PrestoSqlParser::CURRENT_USER) |
               (1ULL << PrestoSqlParser::DATA) |
               (1ULL << PrestoSqlParser::DATE) |
               (1ULL << PrestoSqlParser::DAY) |
               (1ULL << PrestoSqlParser::DEFINER) |
               (1ULL << PrestoSqlParser::DESC) |
               (1ULL << PrestoSqlParser::DETERMINISTIC) |
               (1ULL << PrestoSqlParser::DISABLED) |
               (1ULL << PrestoSqlParser::DISTRIBUTED) |
               (1ULL << PrestoSqlParser::ENABLED) |
               (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) &
              ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::EXISTS - 66)) |
               (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
               (1ULL << (PrestoSqlParser::EXTRACT - 66)) |
               (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
               (1ULL << (PrestoSqlParser::FALSE - 66)) |
               (1ULL << (PrestoSqlParser::FETCH - 66)) |
               (1ULL << (PrestoSqlParser::FILTER - 66)) |
               (1ULL << (PrestoSqlParser::FIRST - 66)) |
               (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
               (1ULL << (PrestoSqlParser::FORMAT - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
               (1ULL << (PrestoSqlParser::GRANT - 66)) |
               (1ULL << (PrestoSqlParser::GRANTED - 66)) |
               (1ULL << (PrestoSqlParser::GRANTS - 66)) |
               (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
               (1ULL << (PrestoSqlParser::GROUPING - 66)) |
               (1ULL << (PrestoSqlParser::GROUPS - 66)) |
               (1ULL << (PrestoSqlParser::HOUR - 66)) |
               (1ULL << (PrestoSqlParser::IF - 66)) |
               (1ULL << (PrestoSqlParser::IGNORE - 66)) |
               (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::INPUT - 66)) |
               (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
               (1ULL << (PrestoSqlParser::INVOKER - 66)) |
               (1ULL << (PrestoSqlParser::IO - 66)) |
               (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
               (1ULL << (PrestoSqlParser::JSON - 66)) |
               (1ULL << (PrestoSqlParser::KEY - 66)) |
               (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
               (1ULL << (PrestoSqlParser::LAST - 66)) |
               (1ULL << (PrestoSqlParser::LATERAL - 66)) |
               (1ULL << (PrestoSqlParser::LEVEL - 66)) |
               (1ULL << (PrestoSqlParser::LIMIT - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIME - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIMESTAMP - 66)) |
               (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
               (1ULL << (PrestoSqlParser::MAP - 66)) |
               (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
               (1ULL << (PrestoSqlParser::MINUTE - 66)) |
               (1ULL << (PrestoSqlParser::MONTH - 66)) |
               (1ULL << (PrestoSqlParser::NAME - 66)) |
               (1ULL << (PrestoSqlParser::NFC - 66)) |
               (1ULL << (PrestoSqlParser::NFD - 66)) |
               (1ULL << (PrestoSqlParser::NFKC - 66)) |
               (1ULL << (PrestoSqlParser::NFKD - 66)) |
               (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) &
              ((1ULL << (PrestoSqlParser::NONE - 130)) |
               (1ULL << (PrestoSqlParser::NORMALIZE - 130)) |
               (1ULL << (PrestoSqlParser::NOT - 130)) |
               (1ULL << (PrestoSqlParser::NULL_LITERAL - 130)) |
               (1ULL << (PrestoSqlParser::NULLIF - 130)) |
               (1ULL << (PrestoSqlParser::NULLS - 130)) |
               (1ULL << (PrestoSqlParser::OF - 130)) |
               (1ULL << (PrestoSqlParser::OFFSET - 130)) |
               (1ULL << (PrestoSqlParser::ONLY - 130)) |
               (1ULL << (PrestoSqlParser::OPTION - 130)) |
               (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
               (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
               (1ULL << (PrestoSqlParser::OVER - 130)) |
               (1ULL << (PrestoSqlParser::PARTITION - 130)) |
               (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
               (1ULL << (PrestoSqlParser::POSITION - 130)) |
               (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
               (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
               (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
               (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
               (1ULL << (PrestoSqlParser::RANGE - 130)) |
               (1ULL << (PrestoSqlParser::READ - 130)) |
               (1ULL << (PrestoSqlParser::REFRESH - 130)) |
               (1ULL << (PrestoSqlParser::RELY - 130)) |
               (1ULL << (PrestoSqlParser::RENAME - 130)) |
               (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
               (1ULL << (PrestoSqlParser::REPLACE - 130)) |
               (1ULL << (PrestoSqlParser::RESET - 130)) |
               (1ULL << (PrestoSqlParser::RESPECT - 130)) |
               (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
               (1ULL << (PrestoSqlParser::RETURN - 130)) |
               (1ULL << (PrestoSqlParser::RETURNS - 130)) |
               (1ULL << (PrestoSqlParser::REVOKE - 130)) |
               (1ULL << (PrestoSqlParser::ROLE - 130)) |
               (1ULL << (PrestoSqlParser::ROLES - 130)) |
               (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
               (1ULL << (PrestoSqlParser::ROW - 130)) |
               (1ULL << (PrestoSqlParser::ROWS - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
               (1ULL << (PrestoSqlParser::SECOND - 130)) |
               (1ULL << (PrestoSqlParser::SECURITY - 130)) |
               (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
               (1ULL << (PrestoSqlParser::SESSION - 130)) |
               (1ULL << (PrestoSqlParser::SET - 130)) |
               (1ULL << (PrestoSqlParser::SETS - 130)) |
               (1ULL << (PrestoSqlParser::SHOW - 130)) |
               (1ULL << (PrestoSqlParser::SOME - 130)) |
               (1ULL << (PrestoSqlParser::SQL - 130)) |
               (1ULL << (PrestoSqlParser::START - 130)) |
               (1ULL << (PrestoSqlParser::STATS - 130)) |
               (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
            ((((_la - 195) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 195)) &
              ((1ULL << (PrestoSqlParser::TABLES - 195)) |
               (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
               (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
               (1ULL << (PrestoSqlParser::TEXT - 195)) |
               (1ULL << (PrestoSqlParser::TIME - 195)) |
               (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
               (1ULL << (PrestoSqlParser::TO - 195)) |
               (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
               (1ULL << (PrestoSqlParser::TRUE - 195)) |
               (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
               (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
               (1ULL << (PrestoSqlParser::TYPE - 195)) |
               (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
               (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
               (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
               (1ULL << (PrestoSqlParser::UPDATE - 195)) |
               (1ULL << (PrestoSqlParser::USE - 195)) |
               (1ULL << (PrestoSqlParser::USER - 195)) |
               (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
               (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
               (1ULL << (PrestoSqlParser::VERSION - 195)) |
               (1ULL << (PrestoSqlParser::VIEW - 195)) |
               (1ULL << (PrestoSqlParser::WORK - 195)) |
               (1ULL << (PrestoSqlParser::WRITE - 195)) |
               (1ULL << (PrestoSqlParser::YEAR - 195)) |
               (1ULL << (PrestoSqlParser::ZONE - 195)) |
               (1ULL << (PrestoSqlParser::PLUS - 195)) |
               (1ULL << (PrestoSqlParser::MINUS - 195)) |
               (1ULL << (PrestoSqlParser::STRING - 195)) |
               (1ULL << (PrestoSqlParser::UNICODE_STRING - 195)) |
               (1ULL << (PrestoSqlParser::BINARY_LITERAL - 195)) |
               (1ULL << (PrestoSqlParser::INTEGER_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DECIMAL_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DIGIT_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::QUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::BACKQUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_PRECISION - 195)))) != 0)) {
          setState(1207);
          expression();
          setState(1212);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1208);
            match(PrestoSqlParser::T__3);
            setState(1209);
            expression();
            setState(1214);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1217);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(1218);
        expression();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NamedQueryContext
//------------------------------------------------------------------

PrestoSqlParser::NamedQueryContext::NamedQueryContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::NamedQueryContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

PrestoSqlParser::QueryContext* PrestoSqlParser::NamedQueryContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::NamedQueryContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::ColumnAliasesContext*
PrestoSqlParser::NamedQueryContext::columnAliases() {
  return getRuleContext<PrestoSqlParser::ColumnAliasesContext>(0);
}

size_t PrestoSqlParser::NamedQueryContext::getRuleIndex() const {
  return PrestoSqlParser::RuleNamedQuery;
}

void PrestoSqlParser::NamedQueryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNamedQuery(this);
}

void PrestoSqlParser::NamedQueryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNamedQuery(this);
}

antlrcpp::Any PrestoSqlParser::NamedQueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNamedQuery(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::NamedQueryContext* PrestoSqlParser::namedQuery() {
  NamedQueryContext* _localctx =
      _tracker.createInstance<NamedQueryContext>(_ctx, getState());
  enterRule(_localctx, 62, PrestoSqlParser::RuleNamedQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1221);
    antlrcpp::downCast<NamedQueryContext*>(_localctx)->name = identifier();
    setState(1223);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::T__1) {
      setState(1222);
      columnAliases();
    }
    setState(1225);
    match(PrestoSqlParser::AS);
    setState(1226);
    match(PrestoSqlParser::T__1);
    setState(1227);
    query();
    setState(1228);
    match(PrestoSqlParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SetQuantifierContext
//------------------------------------------------------------------

PrestoSqlParser::SetQuantifierContext::SetQuantifierContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::SetQuantifierContext::DISTINCT() {
  return getToken(PrestoSqlParser::DISTINCT, 0);
}

tree::TerminalNode* PrestoSqlParser::SetQuantifierContext::ALL() {
  return getToken(PrestoSqlParser::ALL, 0);
}

size_t PrestoSqlParser::SetQuantifierContext::getRuleIndex() const {
  return PrestoSqlParser::RuleSetQuantifier;
}

void PrestoSqlParser::SetQuantifierContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSetQuantifier(this);
}

void PrestoSqlParser::SetQuantifierContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSetQuantifier(this);
}

antlrcpp::Any PrestoSqlParser::SetQuantifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSetQuantifier(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::SetQuantifierContext* PrestoSqlParser::setQuantifier() {
  SetQuantifierContext* _localctx =
      _tracker.createInstance<SetQuantifierContext>(_ctx, getState());
  enterRule(_localctx, 64, PrestoSqlParser::RuleSetQuantifier);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1230);
    _la = _input->LA(1);
    if (!(_la == PrestoSqlParser::ALL

          || _la == PrestoSqlParser::DISTINCT)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectItemContext
//------------------------------------------------------------------

PrestoSqlParser::SelectItemContext::SelectItemContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::SelectItemContext::getRuleIndex() const {
  return PrestoSqlParser::RuleSelectItem;
}

void PrestoSqlParser::SelectItemContext::copyFrom(SelectItemContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- SelectAllContext
//------------------------------------------------------------------

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::SelectAllContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::SelectAllContext::ASTERISK() {
  return getToken(PrestoSqlParser::ASTERISK, 0);
}

PrestoSqlParser::SelectAllContext::SelectAllContext(SelectItemContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SelectAllContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelectAll(this);
}
void PrestoSqlParser::SelectAllContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelectAll(this);
}

antlrcpp::Any PrestoSqlParser::SelectAllContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSelectAll(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SelectSingleContext
//------------------------------------------------------------------

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::SelectSingleContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::SelectSingleContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::SelectSingleContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

PrestoSqlParser::SelectSingleContext::SelectSingleContext(
    SelectItemContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SelectSingleContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelectSingle(this);
}
void PrestoSqlParser::SelectSingleContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelectSingle(this);
}

antlrcpp::Any PrestoSqlParser::SelectSingleContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSelectSingle(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::SelectItemContext* PrestoSqlParser::selectItem() {
  SelectItemContext* _localctx =
      _tracker.createInstance<SelectItemContext>(_ctx, getState());
  enterRule(_localctx, 66, PrestoSqlParser::RuleSelectItem);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1244);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 155, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SelectSingleContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1232);
        expression();
        setState(1237);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 154, _ctx)) {
          case 1: {
            setState(1234);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == PrestoSqlParser::AS) {
              setState(1233);
              match(PrestoSqlParser::AS);
            }
            setState(1236);
            identifier();
            break;
          }

          default:
            break;
        }
        break;
      }

      case 2: {
        _localctx = _tracker.createInstance<PrestoSqlParser::SelectAllContext>(
            _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1239);
        qualifiedName();
        setState(1240);
        match(PrestoSqlParser::T__0);
        setState(1241);
        match(PrestoSqlParser::ASTERISK);
        break;
      }

      case 3: {
        _localctx = _tracker.createInstance<PrestoSqlParser::SelectAllContext>(
            _localctx);
        enterOuterAlt(_localctx, 3);
        setState(1243);
        match(PrestoSqlParser::ASTERISK);
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationContext
//------------------------------------------------------------------

PrestoSqlParser::RelationContext::RelationContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::RelationContext::getRuleIndex() const {
  return PrestoSqlParser::RuleRelation;
}

void PrestoSqlParser::RelationContext::copyFrom(RelationContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- RelationDefaultContext
//------------------------------------------------------------------

PrestoSqlParser::SampledRelationContext*
PrestoSqlParser::RelationDefaultContext::sampledRelation() {
  return getRuleContext<PrestoSqlParser::SampledRelationContext>(0);
}

PrestoSqlParser::RelationDefaultContext::RelationDefaultContext(
    RelationContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RelationDefaultContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRelationDefault(this);
}
void PrestoSqlParser::RelationDefaultContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRelationDefault(this);
}

antlrcpp::Any PrestoSqlParser::RelationDefaultContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRelationDefault(this);
  else
    return visitor->visitChildren(this);
}
//----------------- JoinRelationContext
//------------------------------------------------------------------

std::vector<PrestoSqlParser::RelationContext*>
PrestoSqlParser::JoinRelationContext::relation() {
  return getRuleContexts<PrestoSqlParser::RelationContext>();
}

PrestoSqlParser::RelationContext*
PrestoSqlParser::JoinRelationContext::relation(size_t i) {
  return getRuleContext<PrestoSqlParser::RelationContext>(i);
}

tree::TerminalNode* PrestoSqlParser::JoinRelationContext::CROSS() {
  return getToken(PrestoSqlParser::CROSS, 0);
}

tree::TerminalNode* PrestoSqlParser::JoinRelationContext::JOIN() {
  return getToken(PrestoSqlParser::JOIN, 0);
}

PrestoSqlParser::JoinTypeContext*
PrestoSqlParser::JoinRelationContext::joinType() {
  return getRuleContext<PrestoSqlParser::JoinTypeContext>(0);
}

PrestoSqlParser::JoinCriteriaContext*
PrestoSqlParser::JoinRelationContext::joinCriteria() {
  return getRuleContext<PrestoSqlParser::JoinCriteriaContext>(0);
}

tree::TerminalNode* PrestoSqlParser::JoinRelationContext::NATURAL() {
  return getToken(PrestoSqlParser::NATURAL, 0);
}

PrestoSqlParser::SampledRelationContext*
PrestoSqlParser::JoinRelationContext::sampledRelation() {
  return getRuleContext<PrestoSqlParser::SampledRelationContext>(0);
}

PrestoSqlParser::JoinRelationContext::JoinRelationContext(
    RelationContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::JoinRelationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterJoinRelation(this);
}
void PrestoSqlParser::JoinRelationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitJoinRelation(this);
}

antlrcpp::Any PrestoSqlParser::JoinRelationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitJoinRelation(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::RelationContext* PrestoSqlParser::relation() {
  return relation(0);
}

PrestoSqlParser::RelationContext* PrestoSqlParser::relation(int precedence) {
  ParserRuleContext* parentContext = _ctx;
  size_t parentState = getState();
  PrestoSqlParser::RelationContext* _localctx =
      _tracker.createInstance<RelationContext>(_ctx, parentState);
  PrestoSqlParser::RelationContext* previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by
                         // generated code.
  size_t startState = 68;
  enterRecursionRule(_localctx, 68, PrestoSqlParser::RuleRelation, precedence);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    _localctx = _tracker.createInstance<RelationDefaultContext>(_localctx);
    _ctx = _localctx;
    previousContext = _localctx;

    setState(1247);
    sampledRelation();
    _ctx->stop = _input->LT(-1);
    setState(1267);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 157, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        auto newContext = _tracker.createInstance<JoinRelationContext>(
            _tracker.createInstance<RelationContext>(
                parentContext, parentState));
        _localctx = newContext;
        newContext->left = previousContext;
        pushNewRecursionContext(newContext, startState, RuleRelation);
        setState(1249);

        if (!(precpred(_ctx, 2)))
          throw FailedPredicateException(this, "precpred(_ctx, 2)");
        setState(1263);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case PrestoSqlParser::CROSS: {
            setState(1250);
            match(PrestoSqlParser::CROSS);
            setState(1251);
            match(PrestoSqlParser::JOIN);
            setState(1252);
            antlrcpp::downCast<JoinRelationContext*>(_localctx)->right =
                sampledRelation();
            break;
          }

          case PrestoSqlParser::FULL:
          case PrestoSqlParser::INNER:
          case PrestoSqlParser::JOIN:
          case PrestoSqlParser::LEFT:
          case PrestoSqlParser::RIGHT: {
            setState(1253);
            joinType();
            setState(1254);
            match(PrestoSqlParser::JOIN);
            setState(1255);
            antlrcpp::downCast<JoinRelationContext*>(_localctx)->rightRelation =
                relation(0);
            setState(1256);
            joinCriteria();
            break;
          }

          case PrestoSqlParser::NATURAL: {
            setState(1258);
            match(PrestoSqlParser::NATURAL);
            setState(1259);
            joinType();
            setState(1260);
            match(PrestoSqlParser::JOIN);
            setState(1261);
            antlrcpp::downCast<JoinRelationContext*>(_localctx)->right =
                sampledRelation();
            break;
          }

          default:
            throw NoViableAltException(this);
        }
      }
      setState(1269);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 157, _ctx);
    }
  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- JoinTypeContext
//------------------------------------------------------------------

PrestoSqlParser::JoinTypeContext::JoinTypeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::JoinTypeContext::INNER() {
  return getToken(PrestoSqlParser::INNER, 0);
}

tree::TerminalNode* PrestoSqlParser::JoinTypeContext::LEFT() {
  return getToken(PrestoSqlParser::LEFT, 0);
}

tree::TerminalNode* PrestoSqlParser::JoinTypeContext::OUTER() {
  return getToken(PrestoSqlParser::OUTER, 0);
}

tree::TerminalNode* PrestoSqlParser::JoinTypeContext::RIGHT() {
  return getToken(PrestoSqlParser::RIGHT, 0);
}

tree::TerminalNode* PrestoSqlParser::JoinTypeContext::FULL() {
  return getToken(PrestoSqlParser::FULL, 0);
}

size_t PrestoSqlParser::JoinTypeContext::getRuleIndex() const {
  return PrestoSqlParser::RuleJoinType;
}

void PrestoSqlParser::JoinTypeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterJoinType(this);
}

void PrestoSqlParser::JoinTypeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitJoinType(this);
}

antlrcpp::Any PrestoSqlParser::JoinTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitJoinType(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::JoinTypeContext* PrestoSqlParser::joinType() {
  JoinTypeContext* _localctx =
      _tracker.createInstance<JoinTypeContext>(_ctx, getState());
  enterRule(_localctx, 70, PrestoSqlParser::RuleJoinType);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1285);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::INNER:
      case PrestoSqlParser::JOIN: {
        enterOuterAlt(_localctx, 1);
        setState(1271);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::INNER) {
          setState(1270);
          match(PrestoSqlParser::INNER);
        }
        break;
      }

      case PrestoSqlParser::LEFT: {
        enterOuterAlt(_localctx, 2);
        setState(1273);
        match(PrestoSqlParser::LEFT);
        setState(1275);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::OUTER) {
          setState(1274);
          match(PrestoSqlParser::OUTER);
        }
        break;
      }

      case PrestoSqlParser::RIGHT: {
        enterOuterAlt(_localctx, 3);
        setState(1277);
        match(PrestoSqlParser::RIGHT);
        setState(1279);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::OUTER) {
          setState(1278);
          match(PrestoSqlParser::OUTER);
        }
        break;
      }

      case PrestoSqlParser::FULL: {
        enterOuterAlt(_localctx, 4);
        setState(1281);
        match(PrestoSqlParser::FULL);
        setState(1283);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::OUTER) {
          setState(1282);
          match(PrestoSqlParser::OUTER);
        }
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- JoinCriteriaContext
//------------------------------------------------------------------

PrestoSqlParser::JoinCriteriaContext::JoinCriteriaContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::JoinCriteriaContext::ON() {
  return getToken(PrestoSqlParser::ON, 0);
}

PrestoSqlParser::BooleanExpressionContext*
PrestoSqlParser::JoinCriteriaContext::booleanExpression() {
  return getRuleContext<PrestoSqlParser::BooleanExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::JoinCriteriaContext::USING() {
  return getToken(PrestoSqlParser::USING, 0);
}

std::vector<PrestoSqlParser::IdentifierContext*>
PrestoSqlParser::JoinCriteriaContext::identifier() {
  return getRuleContexts<PrestoSqlParser::IdentifierContext>();
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::JoinCriteriaContext::identifier(size_t i) {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(i);
}

size_t PrestoSqlParser::JoinCriteriaContext::getRuleIndex() const {
  return PrestoSqlParser::RuleJoinCriteria;
}

void PrestoSqlParser::JoinCriteriaContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterJoinCriteria(this);
}

void PrestoSqlParser::JoinCriteriaContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitJoinCriteria(this);
}

antlrcpp::Any PrestoSqlParser::JoinCriteriaContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitJoinCriteria(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::JoinCriteriaContext* PrestoSqlParser::joinCriteria() {
  JoinCriteriaContext* _localctx =
      _tracker.createInstance<JoinCriteriaContext>(_ctx, getState());
  enterRule(_localctx, 72, PrestoSqlParser::RuleJoinCriteria);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1301);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::ON: {
        enterOuterAlt(_localctx, 1);
        setState(1287);
        match(PrestoSqlParser::ON);
        setState(1288);
        booleanExpression(0);
        break;
      }

      case PrestoSqlParser::USING: {
        enterOuterAlt(_localctx, 2);
        setState(1289);
        match(PrestoSqlParser::USING);
        setState(1290);
        match(PrestoSqlParser::T__1);
        setState(1291);
        identifier();
        setState(1296);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1292);
          match(PrestoSqlParser::T__3);
          setState(1293);
          identifier();
          setState(1298);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1299);
        match(PrestoSqlParser::T__2);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SampledRelationContext
//------------------------------------------------------------------

PrestoSqlParser::SampledRelationContext::SampledRelationContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::AliasedRelationContext*
PrestoSqlParser::SampledRelationContext::aliasedRelation() {
  return getRuleContext<PrestoSqlParser::AliasedRelationContext>(0);
}

tree::TerminalNode* PrestoSqlParser::SampledRelationContext::TABLESAMPLE() {
  return getToken(PrestoSqlParser::TABLESAMPLE, 0);
}

PrestoSqlParser::SampleTypeContext*
PrestoSqlParser::SampledRelationContext::sampleType() {
  return getRuleContext<PrestoSqlParser::SampleTypeContext>(0);
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::SampledRelationContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

size_t PrestoSqlParser::SampledRelationContext::getRuleIndex() const {
  return PrestoSqlParser::RuleSampledRelation;
}

void PrestoSqlParser::SampledRelationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSampledRelation(this);
}

void PrestoSqlParser::SampledRelationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSampledRelation(this);
}

antlrcpp::Any PrestoSqlParser::SampledRelationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSampledRelation(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::SampledRelationContext* PrestoSqlParser::sampledRelation() {
  SampledRelationContext* _localctx =
      _tracker.createInstance<SampledRelationContext>(_ctx, getState());
  enterRule(_localctx, 74, PrestoSqlParser::RuleSampledRelation);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1303);
    aliasedRelation();
    setState(1310);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 165, _ctx)) {
      case 1: {
        setState(1304);
        match(PrestoSqlParser::TABLESAMPLE);
        setState(1305);
        sampleType();
        setState(1306);
        match(PrestoSqlParser::T__1);
        setState(1307);
        antlrcpp::downCast<SampledRelationContext*>(_localctx)->percentage =
            expression();
        setState(1308);
        match(PrestoSqlParser::T__2);
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SampleTypeContext
//------------------------------------------------------------------

PrestoSqlParser::SampleTypeContext::SampleTypeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::SampleTypeContext::BERNOULLI() {
  return getToken(PrestoSqlParser::BERNOULLI, 0);
}

tree::TerminalNode* PrestoSqlParser::SampleTypeContext::SYSTEM() {
  return getToken(PrestoSqlParser::SYSTEM, 0);
}

size_t PrestoSqlParser::SampleTypeContext::getRuleIndex() const {
  return PrestoSqlParser::RuleSampleType;
}

void PrestoSqlParser::SampleTypeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSampleType(this);
}

void PrestoSqlParser::SampleTypeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSampleType(this);
}

antlrcpp::Any PrestoSqlParser::SampleTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSampleType(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::SampleTypeContext* PrestoSqlParser::sampleType() {
  SampleTypeContext* _localctx =
      _tracker.createInstance<SampleTypeContext>(_ctx, getState());
  enterRule(_localctx, 76, PrestoSqlParser::RuleSampleType);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1312);
    _la = _input->LA(1);
    if (!(_la == PrestoSqlParser::BERNOULLI ||
          _la == PrestoSqlParser::SYSTEM)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AliasedRelationContext
//------------------------------------------------------------------

PrestoSqlParser::AliasedRelationContext::AliasedRelationContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::RelationPrimaryContext*
PrestoSqlParser::AliasedRelationContext::relationPrimary() {
  return getRuleContext<PrestoSqlParser::RelationPrimaryContext>(0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::AliasedRelationContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::AliasedRelationContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

PrestoSqlParser::ColumnAliasesContext*
PrestoSqlParser::AliasedRelationContext::columnAliases() {
  return getRuleContext<PrestoSqlParser::ColumnAliasesContext>(0);
}

size_t PrestoSqlParser::AliasedRelationContext::getRuleIndex() const {
  return PrestoSqlParser::RuleAliasedRelation;
}

void PrestoSqlParser::AliasedRelationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAliasedRelation(this);
}

void PrestoSqlParser::AliasedRelationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitAliasedRelation(this);
}

antlrcpp::Any PrestoSqlParser::AliasedRelationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAliasedRelation(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::AliasedRelationContext* PrestoSqlParser::aliasedRelation() {
  AliasedRelationContext* _localctx =
      _tracker.createInstance<AliasedRelationContext>(_ctx, getState());
  enterRule(_localctx, 78, PrestoSqlParser::RuleAliasedRelation);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1314);
    relationPrimary();
    setState(1322);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 168, _ctx)) {
      case 1: {
        setState(1316);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::AS) {
          setState(1315);
          match(PrestoSqlParser::AS);
        }
        setState(1318);
        identifier();
        setState(1320);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 167, _ctx)) {
          case 1: {
            setState(1319);
            columnAliases();
            break;
          }

          default:
            break;
        }
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ColumnAliasesContext
//------------------------------------------------------------------

PrestoSqlParser::ColumnAliasesContext::ColumnAliasesContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::IdentifierContext*>
PrestoSqlParser::ColumnAliasesContext::identifier() {
  return getRuleContexts<PrestoSqlParser::IdentifierContext>();
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ColumnAliasesContext::identifier(size_t i) {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(i);
}

size_t PrestoSqlParser::ColumnAliasesContext::getRuleIndex() const {
  return PrestoSqlParser::RuleColumnAliases;
}

void PrestoSqlParser::ColumnAliasesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterColumnAliases(this);
}

void PrestoSqlParser::ColumnAliasesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitColumnAliases(this);
}

antlrcpp::Any PrestoSqlParser::ColumnAliasesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitColumnAliases(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ColumnAliasesContext* PrestoSqlParser::columnAliases() {
  ColumnAliasesContext* _localctx =
      _tracker.createInstance<ColumnAliasesContext>(_ctx, getState());
  enterRule(_localctx, 80, PrestoSqlParser::RuleColumnAliases);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1324);
    match(PrestoSqlParser::T__1);
    setState(1325);
    identifier();
    setState(1330);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::T__3) {
      setState(1326);
      match(PrestoSqlParser::T__3);
      setState(1327);
      identifier();
      setState(1332);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(1333);
    match(PrestoSqlParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationPrimaryContext
//------------------------------------------------------------------

PrestoSqlParser::RelationPrimaryContext::RelationPrimaryContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::RelationPrimaryContext::getRuleIndex() const {
  return PrestoSqlParser::RuleRelationPrimary;
}

void PrestoSqlParser::RelationPrimaryContext::copyFrom(
    RelationPrimaryContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- SubqueryRelationContext
//------------------------------------------------------------------

PrestoSqlParser::QueryContext*
PrestoSqlParser::SubqueryRelationContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

PrestoSqlParser::SubqueryRelationContext::SubqueryRelationContext(
    RelationPrimaryContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SubqueryRelationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSubqueryRelation(this);
}
void PrestoSqlParser::SubqueryRelationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSubqueryRelation(this);
}

antlrcpp::Any PrestoSqlParser::SubqueryRelationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSubqueryRelation(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ParenthesizedRelationContext
//------------------------------------------------------------------

PrestoSqlParser::RelationContext*
PrestoSqlParser::ParenthesizedRelationContext::relation() {
  return getRuleContext<PrestoSqlParser::RelationContext>(0);
}

PrestoSqlParser::ParenthesizedRelationContext::ParenthesizedRelationContext(
    RelationPrimaryContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ParenthesizedRelationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterParenthesizedRelation(this);
}
void PrestoSqlParser::ParenthesizedRelationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitParenthesizedRelation(this);
}

antlrcpp::Any PrestoSqlParser::ParenthesizedRelationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitParenthesizedRelation(this);
  else
    return visitor->visitChildren(this);
}
//----------------- UnnestContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::UnnestContext::UNNEST() {
  return getToken(PrestoSqlParser::UNNEST, 0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::UnnestContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext* PrestoSqlParser::UnnestContext::expression(
    size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

tree::TerminalNode* PrestoSqlParser::UnnestContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

tree::TerminalNode* PrestoSqlParser::UnnestContext::ORDINALITY() {
  return getToken(PrestoSqlParser::ORDINALITY, 0);
}

PrestoSqlParser::UnnestContext::UnnestContext(RelationPrimaryContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::UnnestContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnnest(this);
}
void PrestoSqlParser::UnnestContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnnest(this);
}

antlrcpp::Any PrestoSqlParser::UnnestContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUnnest(this);
  else
    return visitor->visitChildren(this);
}
//----------------- LateralContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::LateralContext::LATERAL() {
  return getToken(PrestoSqlParser::LATERAL, 0);
}

PrestoSqlParser::QueryContext* PrestoSqlParser::LateralContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

PrestoSqlParser::LateralContext::LateralContext(RelationPrimaryContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::LateralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterLateral(this);
}
void PrestoSqlParser::LateralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitLateral(this);
}

antlrcpp::Any PrestoSqlParser::LateralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitLateral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableNameContext
//------------------------------------------------------------------

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::TableNameContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

PrestoSqlParser::TableVersionExpressionContext*
PrestoSqlParser::TableNameContext::tableVersionExpression() {
  return getRuleContext<PrestoSqlParser::TableVersionExpressionContext>(0);
}

PrestoSqlParser::TableNameContext::TableNameContext(
    RelationPrimaryContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::TableNameContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTableName(this);
}
void PrestoSqlParser::TableNameContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTableName(this);
}

antlrcpp::Any PrestoSqlParser::TableNameContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTableName(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::RelationPrimaryContext* PrestoSqlParser::relationPrimary() {
  RelationPrimaryContext* _localctx =
      _tracker.createInstance<RelationPrimaryContext>(_ctx, getState());
  enterRule(_localctx, 82, PrestoSqlParser::RuleRelationPrimary);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1367);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 173, _ctx)) {
      case 1: {
        _localctx = _tracker.createInstance<PrestoSqlParser::TableNameContext>(
            _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1335);
        qualifiedName();
        setState(1337);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 170, _ctx)) {
          case 1: {
            setState(1336);
            tableVersionExpression();
            break;
          }

          default:
            break;
        }
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SubqueryRelationContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1339);
        match(PrestoSqlParser::T__1);
        setState(1340);
        query();
        setState(1341);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UnnestContext>(_localctx);
        enterOuterAlt(_localctx, 3);
        setState(1343);
        match(PrestoSqlParser::UNNEST);
        setState(1344);
        match(PrestoSqlParser::T__1);
        setState(1345);
        expression();
        setState(1350);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1346);
          match(PrestoSqlParser::T__3);
          setState(1347);
          expression();
          setState(1352);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1353);
        match(PrestoSqlParser::T__2);
        setState(1356);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 172, _ctx)) {
          case 1: {
            setState(1354);
            match(PrestoSqlParser::WITH);
            setState(1355);
            match(PrestoSqlParser::ORDINALITY);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 4: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::LateralContext>(_localctx);
        enterOuterAlt(_localctx, 4);
        setState(1358);
        match(PrestoSqlParser::LATERAL);
        setState(1359);
        match(PrestoSqlParser::T__1);
        setState(1360);
        query();
        setState(1361);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 5: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::ParenthesizedRelationContext>(
                    _localctx);
        enterOuterAlt(_localctx, 5);
        setState(1363);
        match(PrestoSqlParser::T__1);
        setState(1364);
        relation(0);
        setState(1365);
        match(PrestoSqlParser::T__2);
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExpressionContext
//------------------------------------------------------------------

PrestoSqlParser::ExpressionContext::ExpressionContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::BooleanExpressionContext*
PrestoSqlParser::ExpressionContext::booleanExpression() {
  return getRuleContext<PrestoSqlParser::BooleanExpressionContext>(0);
}

size_t PrestoSqlParser::ExpressionContext::getRuleIndex() const {
  return PrestoSqlParser::RuleExpression;
}

void PrestoSqlParser::ExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpression(this);
}

void PrestoSqlParser::ExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpression(this);
}

antlrcpp::Any PrestoSqlParser::ExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExpression(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ExpressionContext* PrestoSqlParser::expression() {
  ExpressionContext* _localctx =
      _tracker.createInstance<ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 84, PrestoSqlParser::RuleExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1369);
    booleanExpression(0);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BooleanExpressionContext
//------------------------------------------------------------------

PrestoSqlParser::BooleanExpressionContext::BooleanExpressionContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::BooleanExpressionContext::getRuleIndex() const {
  return PrestoSqlParser::RuleBooleanExpression;
}

void PrestoSqlParser::BooleanExpressionContext::copyFrom(
    BooleanExpressionContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- LogicalNotContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::LogicalNotContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

PrestoSqlParser::BooleanExpressionContext*
PrestoSqlParser::LogicalNotContext::booleanExpression() {
  return getRuleContext<PrestoSqlParser::BooleanExpressionContext>(0);
}

PrestoSqlParser::LogicalNotContext::LogicalNotContext(
    BooleanExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::LogicalNotContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterLogicalNot(this);
}
void PrestoSqlParser::LogicalNotContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitLogicalNot(this);
}

antlrcpp::Any PrestoSqlParser::LogicalNotContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitLogicalNot(this);
  else
    return visitor->visitChildren(this);
}
//----------------- PredicatedContext
//------------------------------------------------------------------

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::PredicatedContext::valueExpression() {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(0);
}

PrestoSqlParser::PredicateContext*
PrestoSqlParser::PredicatedContext::predicate() {
  return getRuleContext<PrestoSqlParser::PredicateContext>(0);
}

PrestoSqlParser::PredicatedContext::PredicatedContext(
    BooleanExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::PredicatedContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterPredicated(this);
}
void PrestoSqlParser::PredicatedContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitPredicated(this);
}

antlrcpp::Any PrestoSqlParser::PredicatedContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitPredicated(this);
  else
    return visitor->visitChildren(this);
}
//----------------- LogicalBinaryContext
//------------------------------------------------------------------

std::vector<PrestoSqlParser::BooleanExpressionContext*>
PrestoSqlParser::LogicalBinaryContext::booleanExpression() {
  return getRuleContexts<PrestoSqlParser::BooleanExpressionContext>();
}

PrestoSqlParser::BooleanExpressionContext*
PrestoSqlParser::LogicalBinaryContext::booleanExpression(size_t i) {
  return getRuleContext<PrestoSqlParser::BooleanExpressionContext>(i);
}

tree::TerminalNode* PrestoSqlParser::LogicalBinaryContext::AND() {
  return getToken(PrestoSqlParser::AND, 0);
}

tree::TerminalNode* PrestoSqlParser::LogicalBinaryContext::OR() {
  return getToken(PrestoSqlParser::OR, 0);
}

PrestoSqlParser::LogicalBinaryContext::LogicalBinaryContext(
    BooleanExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::LogicalBinaryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterLogicalBinary(this);
}
void PrestoSqlParser::LogicalBinaryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitLogicalBinary(this);
}

antlrcpp::Any PrestoSqlParser::LogicalBinaryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitLogicalBinary(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::BooleanExpressionContext*
PrestoSqlParser::booleanExpression() {
  return booleanExpression(0);
}

PrestoSqlParser::BooleanExpressionContext* PrestoSqlParser::booleanExpression(
    int precedence) {
  ParserRuleContext* parentContext = _ctx;
  size_t parentState = getState();
  PrestoSqlParser::BooleanExpressionContext* _localctx =
      _tracker.createInstance<BooleanExpressionContext>(_ctx, parentState);
  PrestoSqlParser::BooleanExpressionContext* previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by
                         // generated code.
  size_t startState = 86;
  enterRecursionRule(
      _localctx, 86, PrestoSqlParser::RuleBooleanExpression, precedence);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1378);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::T__1:
      case PrestoSqlParser::T__4:
      case PrestoSqlParser::ADD:
      case PrestoSqlParser::ADMIN:
      case PrestoSqlParser::ALL:
      case PrestoSqlParser::ANALYZE:
      case PrestoSqlParser::ANY:
      case PrestoSqlParser::ARRAY:
      case PrestoSqlParser::ASC:
      case PrestoSqlParser::AT:
      case PrestoSqlParser::BEFORE:
      case PrestoSqlParser::BERNOULLI:
      case PrestoSqlParser::CALL:
      case PrestoSqlParser::CALLED:
      case PrestoSqlParser::CASCADE:
      case PrestoSqlParser::CASE:
      case PrestoSqlParser::CAST:
      case PrestoSqlParser::CATALOGS:
      case PrestoSqlParser::COLUMN:
      case PrestoSqlParser::COLUMNS:
      case PrestoSqlParser::COMMENT:
      case PrestoSqlParser::COMMIT:
      case PrestoSqlParser::COMMITTED:
      case PrestoSqlParser::CURRENT:
      case PrestoSqlParser::CURRENT_DATE:
      case PrestoSqlParser::CURRENT_ROLE:
      case PrestoSqlParser::CURRENT_TIME:
      case PrestoSqlParser::CURRENT_TIMESTAMP:
      case PrestoSqlParser::CURRENT_USER:
      case PrestoSqlParser::DATA:
      case PrestoSqlParser::DATE:
      case PrestoSqlParser::DAY:
      case PrestoSqlParser::DEFINER:
      case PrestoSqlParser::DESC:
      case PrestoSqlParser::DETERMINISTIC:
      case PrestoSqlParser::DISABLED:
      case PrestoSqlParser::DISTRIBUTED:
      case PrestoSqlParser::ENABLED:
      case PrestoSqlParser::ENFORCED:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXISTS:
      case PrestoSqlParser::EXPLAIN:
      case PrestoSqlParser::EXTRACT:
      case PrestoSqlParser::EXTERNAL:
      case PrestoSqlParser::FALSE:
      case PrestoSqlParser::FETCH:
      case PrestoSqlParser::FILTER:
      case PrestoSqlParser::FIRST:
      case PrestoSqlParser::FOLLOWING:
      case PrestoSqlParser::FORMAT:
      case PrestoSqlParser::FUNCTION:
      case PrestoSqlParser::FUNCTIONS:
      case PrestoSqlParser::GRANT:
      case PrestoSqlParser::GRANTED:
      case PrestoSqlParser::GRANTS:
      case PrestoSqlParser::GRAPHVIZ:
      case PrestoSqlParser::GROUPING:
      case PrestoSqlParser::GROUPS:
      case PrestoSqlParser::HOUR:
      case PrestoSqlParser::IF:
      case PrestoSqlParser::IGNORE:
      case PrestoSqlParser::INCLUDING:
      case PrestoSqlParser::INPUT:
      case PrestoSqlParser::INTERVAL:
      case PrestoSqlParser::INVOKER:
      case PrestoSqlParser::IO:
      case PrestoSqlParser::ISOLATION:
      case PrestoSqlParser::JSON:
      case PrestoSqlParser::KEY:
      case PrestoSqlParser::LANGUAGE:
      case PrestoSqlParser::LAST:
      case PrestoSqlParser::LATERAL:
      case PrestoSqlParser::LEVEL:
      case PrestoSqlParser::LIMIT:
      case PrestoSqlParser::LOCALTIME:
      case PrestoSqlParser::LOCALTIMESTAMP:
      case PrestoSqlParser::LOGICAL:
      case PrestoSqlParser::MAP:
      case PrestoSqlParser::MATERIALIZED:
      case PrestoSqlParser::MINUTE:
      case PrestoSqlParser::MONTH:
      case PrestoSqlParser::NAME:
      case PrestoSqlParser::NFC:
      case PrestoSqlParser::NFD:
      case PrestoSqlParser::NFKC:
      case PrestoSqlParser::NFKD:
      case PrestoSqlParser::NO:
      case PrestoSqlParser::NONE:
      case PrestoSqlParser::NORMALIZE:
      case PrestoSqlParser::NULL_LITERAL:
      case PrestoSqlParser::NULLIF:
      case PrestoSqlParser::NULLS:
      case PrestoSqlParser::OF:
      case PrestoSqlParser::OFFSET:
      case PrestoSqlParser::ONLY:
      case PrestoSqlParser::OPTION:
      case PrestoSqlParser::ORDINALITY:
      case PrestoSqlParser::OUTPUT:
      case PrestoSqlParser::OVER:
      case PrestoSqlParser::PARTITION:
      case PrestoSqlParser::PARTITIONS:
      case PrestoSqlParser::POSITION:
      case PrestoSqlParser::PRECEDING:
      case PrestoSqlParser::PRIMARY:
      case PrestoSqlParser::PRIVILEGES:
      case PrestoSqlParser::PROPERTIES:
      case PrestoSqlParser::RANGE:
      case PrestoSqlParser::READ:
      case PrestoSqlParser::REFRESH:
      case PrestoSqlParser::RELY:
      case PrestoSqlParser::RENAME:
      case PrestoSqlParser::REPEATABLE:
      case PrestoSqlParser::REPLACE:
      case PrestoSqlParser::RESET:
      case PrestoSqlParser::RESPECT:
      case PrestoSqlParser::RESTRICT:
      case PrestoSqlParser::RETURN:
      case PrestoSqlParser::RETURNS:
      case PrestoSqlParser::REVOKE:
      case PrestoSqlParser::ROLE:
      case PrestoSqlParser::ROLES:
      case PrestoSqlParser::ROLLBACK:
      case PrestoSqlParser::ROW:
      case PrestoSqlParser::ROWS:
      case PrestoSqlParser::SCHEMA:
      case PrestoSqlParser::SCHEMAS:
      case PrestoSqlParser::SECOND:
      case PrestoSqlParser::SECURITY:
      case PrestoSqlParser::SERIALIZABLE:
      case PrestoSqlParser::SESSION:
      case PrestoSqlParser::SET:
      case PrestoSqlParser::SETS:
      case PrestoSqlParser::SHOW:
      case PrestoSqlParser::SOME:
      case PrestoSqlParser::SQL:
      case PrestoSqlParser::START:
      case PrestoSqlParser::STATS:
      case PrestoSqlParser::SUBSTRING:
      case PrestoSqlParser::SYSTEM:
      case PrestoSqlParser::SYSTEM_TIME:
      case PrestoSqlParser::SYSTEM_VERSION:
      case PrestoSqlParser::TABLES:
      case PrestoSqlParser::TABLESAMPLE:
      case PrestoSqlParser::TEMPORARY:
      case PrestoSqlParser::TEXT:
      case PrestoSqlParser::TIME:
      case PrestoSqlParser::TIMESTAMP:
      case PrestoSqlParser::TO:
      case PrestoSqlParser::TRANSACTION:
      case PrestoSqlParser::TRUE:
      case PrestoSqlParser::TRUNCATE:
      case PrestoSqlParser::TRY_CAST:
      case PrestoSqlParser::TYPE:
      case PrestoSqlParser::UNBOUNDED:
      case PrestoSqlParser::UNCOMMITTED:
      case PrestoSqlParser::UNIQUE:
      case PrestoSqlParser::UPDATE:
      case PrestoSqlParser::USE:
      case PrestoSqlParser::USER:
      case PrestoSqlParser::VALIDATE:
      case PrestoSqlParser::VERBOSE:
      case PrestoSqlParser::VERSION:
      case PrestoSqlParser::VIEW:
      case PrestoSqlParser::WORK:
      case PrestoSqlParser::WRITE:
      case PrestoSqlParser::YEAR:
      case PrestoSqlParser::ZONE:
      case PrestoSqlParser::PLUS:
      case PrestoSqlParser::MINUS:
      case PrestoSqlParser::STRING:
      case PrestoSqlParser::UNICODE_STRING:
      case PrestoSqlParser::BINARY_LITERAL:
      case PrestoSqlParser::INTEGER_VALUE:
      case PrestoSqlParser::DECIMAL_VALUE:
      case PrestoSqlParser::DOUBLE_VALUE:
      case PrestoSqlParser::IDENTIFIER:
      case PrestoSqlParser::DIGIT_IDENTIFIER:
      case PrestoSqlParser::QUOTED_IDENTIFIER:
      case PrestoSqlParser::BACKQUOTED_IDENTIFIER:
      case PrestoSqlParser::DOUBLE_PRECISION: {
        _localctx = _tracker.createInstance<PredicatedContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;

        setState(1372);
        antlrcpp::downCast<PredicatedContext*>(_localctx)
            ->valueExpressionContext = valueExpression(0);
        setState(1374);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 174, _ctx)) {
          case 1: {
            setState(1373);
            predicate(antlrcpp::downCast<PredicatedContext*>(_localctx)
                          ->valueExpressionContext);
            break;
          }

          default:
            break;
        }
        break;
      }

      case PrestoSqlParser::NOT: {
        _localctx = _tracker.createInstance<LogicalNotContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1376);
        match(PrestoSqlParser::NOT);
        setState(1377);
        booleanExpression(3);
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    _ctx->stop = _input->LT(-1);
    setState(1388);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 177, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1386);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 176, _ctx)) {
          case 1: {
            auto newContext = _tracker.createInstance<LogicalBinaryContext>(
                _tracker.createInstance<BooleanExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->left = previousContext;
            pushNewRecursionContext(
                newContext, startState, RuleBooleanExpression);
            setState(1380);

            if (!(precpred(_ctx, 2)))
              throw FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(1381);
            antlrcpp::downCast<LogicalBinaryContext*>(_localctx)->op =
                match(PrestoSqlParser::AND);
            setState(1382);
            antlrcpp::downCast<LogicalBinaryContext*>(_localctx)->right =
                booleanExpression(3);
            break;
          }

          case 2: {
            auto newContext = _tracker.createInstance<LogicalBinaryContext>(
                _tracker.createInstance<BooleanExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->left = previousContext;
            pushNewRecursionContext(
                newContext, startState, RuleBooleanExpression);
            setState(1383);

            if (!(precpred(_ctx, 1)))
              throw FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(1384);
            antlrcpp::downCast<LogicalBinaryContext*>(_localctx)->op =
                match(PrestoSqlParser::OR);
            setState(1385);
            antlrcpp::downCast<LogicalBinaryContext*>(_localctx)->right =
                booleanExpression(2);
            break;
          }

          default:
            break;
        }
      }
      setState(1390);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 177, _ctx);
    }
  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- PredicateContext
//------------------------------------------------------------------

PrestoSqlParser::PredicateContext::PredicateContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::PredicateContext::PredicateContext(
    ParserRuleContext* parent,
    size_t invokingState,
    antlr4::ParserRuleContext* value)
    : ParserRuleContext(parent, invokingState) {
  this->value = value;
}

size_t PrestoSqlParser::PredicateContext::getRuleIndex() const {
  return PrestoSqlParser::RulePredicate;
}

void PrestoSqlParser::PredicateContext::copyFrom(PredicateContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
  this->value = ctx->value;
}

//----------------- ComparisonContext
//------------------------------------------------------------------

PrestoSqlParser::ComparisonOperatorContext*
PrestoSqlParser::ComparisonContext::comparisonOperator() {
  return getRuleContext<PrestoSqlParser::ComparisonOperatorContext>(0);
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::ComparisonContext::valueExpression() {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(0);
}

PrestoSqlParser::ComparisonContext::ComparisonContext(PredicateContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ComparisonContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterComparison(this);
}
void PrestoSqlParser::ComparisonContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitComparison(this);
}

antlrcpp::Any PrestoSqlParser::ComparisonContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitComparison(this);
  else
    return visitor->visitChildren(this);
}
//----------------- LikeContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::LikeContext::LIKE() {
  return getToken(PrestoSqlParser::LIKE, 0);
}

std::vector<PrestoSqlParser::ValueExpressionContext*>
PrestoSqlParser::LikeContext::valueExpression() {
  return getRuleContexts<PrestoSqlParser::ValueExpressionContext>();
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::LikeContext::valueExpression(size_t i) {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(i);
}

tree::TerminalNode* PrestoSqlParser::LikeContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

tree::TerminalNode* PrestoSqlParser::LikeContext::ESCAPE() {
  return getToken(PrestoSqlParser::ESCAPE, 0);
}

PrestoSqlParser::LikeContext::LikeContext(PredicateContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::LikeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterLike(this);
}
void PrestoSqlParser::LikeContext::exitRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitLike(this);
}

antlrcpp::Any PrestoSqlParser::LikeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitLike(this);
  else
    return visitor->visitChildren(this);
}
//----------------- InSubqueryContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::InSubqueryContext::IN() {
  return getToken(PrestoSqlParser::IN, 0);
}

PrestoSqlParser::QueryContext* PrestoSqlParser::InSubqueryContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

tree::TerminalNode* PrestoSqlParser::InSubqueryContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

PrestoSqlParser::InSubqueryContext::InSubqueryContext(PredicateContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::InSubqueryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterInSubquery(this);
}
void PrestoSqlParser::InSubqueryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitInSubquery(this);
}

antlrcpp::Any PrestoSqlParser::InSubqueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitInSubquery(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DistinctFromContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DistinctFromContext::IS() {
  return getToken(PrestoSqlParser::IS, 0);
}

tree::TerminalNode* PrestoSqlParser::DistinctFromContext::DISTINCT() {
  return getToken(PrestoSqlParser::DISTINCT, 0);
}

tree::TerminalNode* PrestoSqlParser::DistinctFromContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::DistinctFromContext::valueExpression() {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::DistinctFromContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

PrestoSqlParser::DistinctFromContext::DistinctFromContext(
    PredicateContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DistinctFromContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDistinctFrom(this);
}
void PrestoSqlParser::DistinctFromContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDistinctFrom(this);
}

antlrcpp::Any PrestoSqlParser::DistinctFromContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDistinctFrom(this);
  else
    return visitor->visitChildren(this);
}
//----------------- InListContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::InListContext::IN() {
  return getToken(PrestoSqlParser::IN, 0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::InListContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext* PrestoSqlParser::InListContext::expression(
    size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

tree::TerminalNode* PrestoSqlParser::InListContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

PrestoSqlParser::InListContext::InListContext(PredicateContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::InListContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterInList(this);
}
void PrestoSqlParser::InListContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitInList(this);
}

antlrcpp::Any PrestoSqlParser::InListContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitInList(this);
  else
    return visitor->visitChildren(this);
}
//----------------- NullPredicateContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::NullPredicateContext::IS() {
  return getToken(PrestoSqlParser::IS, 0);
}

tree::TerminalNode* PrestoSqlParser::NullPredicateContext::NULL_LITERAL() {
  return getToken(PrestoSqlParser::NULL_LITERAL, 0);
}

tree::TerminalNode* PrestoSqlParser::NullPredicateContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

PrestoSqlParser::NullPredicateContext::NullPredicateContext(
    PredicateContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::NullPredicateContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNullPredicate(this);
}
void PrestoSqlParser::NullPredicateContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNullPredicate(this);
}

antlrcpp::Any PrestoSqlParser::NullPredicateContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNullPredicate(this);
  else
    return visitor->visitChildren(this);
}
//----------------- BetweenContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::BetweenContext::BETWEEN() {
  return getToken(PrestoSqlParser::BETWEEN, 0);
}

tree::TerminalNode* PrestoSqlParser::BetweenContext::AND() {
  return getToken(PrestoSqlParser::AND, 0);
}

std::vector<PrestoSqlParser::ValueExpressionContext*>
PrestoSqlParser::BetweenContext::valueExpression() {
  return getRuleContexts<PrestoSqlParser::ValueExpressionContext>();
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::BetweenContext::valueExpression(size_t i) {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(i);
}

tree::TerminalNode* PrestoSqlParser::BetweenContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

PrestoSqlParser::BetweenContext::BetweenContext(PredicateContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::BetweenContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBetween(this);
}
void PrestoSqlParser::BetweenContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitBetween(this);
}

antlrcpp::Any PrestoSqlParser::BetweenContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBetween(this);
  else
    return visitor->visitChildren(this);
}
//----------------- QuantifiedComparisonContext
//------------------------------------------------------------------

PrestoSqlParser::ComparisonOperatorContext*
PrestoSqlParser::QuantifiedComparisonContext::comparisonOperator() {
  return getRuleContext<PrestoSqlParser::ComparisonOperatorContext>(0);
}

PrestoSqlParser::ComparisonQuantifierContext*
PrestoSqlParser::QuantifiedComparisonContext::comparisonQuantifier() {
  return getRuleContext<PrestoSqlParser::ComparisonQuantifierContext>(0);
}

PrestoSqlParser::QueryContext*
PrestoSqlParser::QuantifiedComparisonContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

PrestoSqlParser::QuantifiedComparisonContext::QuantifiedComparisonContext(
    PredicateContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::QuantifiedComparisonContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuantifiedComparison(this);
}
void PrestoSqlParser::QuantifiedComparisonContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuantifiedComparison(this);
}

antlrcpp::Any PrestoSqlParser::QuantifiedComparisonContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitQuantifiedComparison(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::PredicateContext* PrestoSqlParser::predicate(
    antlr4::ParserRuleContext* value) {
  PredicateContext* _localctx =
      _tracker.createInstance<PredicateContext>(_ctx, getState(), value);
  enterRule(_localctx, 88, PrestoSqlParser::RulePredicate);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1452);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 186, _ctx)) {
      case 1: {
        _localctx = _tracker.createInstance<PrestoSqlParser::ComparisonContext>(
            _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1391);
        comparisonOperator();
        setState(1392);
        antlrcpp::downCast<ComparisonContext*>(_localctx)->right =
            valueExpression(0);
        break;
      }

      case 2: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::QuantifiedComparisonContext>(
                    _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1394);
        comparisonOperator();
        setState(1395);
        comparisonQuantifier();
        setState(1396);
        match(PrestoSqlParser::T__1);
        setState(1397);
        query();
        setState(1398);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::BetweenContext>(_localctx);
        enterOuterAlt(_localctx, 3);
        setState(1401);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1400);
          match(PrestoSqlParser::NOT);
        }
        setState(1403);
        match(PrestoSqlParser::BETWEEN);
        setState(1404);
        antlrcpp::downCast<BetweenContext*>(_localctx)->lower =
            valueExpression(0);
        setState(1405);
        match(PrestoSqlParser::AND);
        setState(1406);
        antlrcpp::downCast<BetweenContext*>(_localctx)->upper =
            valueExpression(0);
        break;
      }

      case 4: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::InListContext>(_localctx);
        enterOuterAlt(_localctx, 4);
        setState(1409);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1408);
          match(PrestoSqlParser::NOT);
        }
        setState(1411);
        match(PrestoSqlParser::IN);
        setState(1412);
        match(PrestoSqlParser::T__1);
        setState(1413);
        expression();
        setState(1418);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1414);
          match(PrestoSqlParser::T__3);
          setState(1415);
          expression();
          setState(1420);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1421);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 5: {
        _localctx = _tracker.createInstance<PrestoSqlParser::InSubqueryContext>(
            _localctx);
        enterOuterAlt(_localctx, 5);
        setState(1424);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1423);
          match(PrestoSqlParser::NOT);
        }
        setState(1426);
        match(PrestoSqlParser::IN);
        setState(1427);
        match(PrestoSqlParser::T__1);
        setState(1428);
        query();
        setState(1429);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 6: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::LikeContext>(_localctx);
        enterOuterAlt(_localctx, 6);
        setState(1432);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1431);
          match(PrestoSqlParser::NOT);
        }
        setState(1434);
        match(PrestoSqlParser::LIKE);
        setState(1435);
        antlrcpp::downCast<LikeContext*>(_localctx)->pattern =
            valueExpression(0);
        setState(1438);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 183, _ctx)) {
          case 1: {
            setState(1436);
            match(PrestoSqlParser::ESCAPE);
            setState(1437);
            antlrcpp::downCast<LikeContext*>(_localctx)->escape =
                valueExpression(0);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 7: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::NullPredicateContext>(
                _localctx);
        enterOuterAlt(_localctx, 7);
        setState(1440);
        match(PrestoSqlParser::IS);
        setState(1442);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1441);
          match(PrestoSqlParser::NOT);
        }
        setState(1444);
        match(PrestoSqlParser::NULL_LITERAL);
        break;
      }

      case 8: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DistinctFromContext>(
                _localctx);
        enterOuterAlt(_localctx, 8);
        setState(1445);
        match(PrestoSqlParser::IS);
        setState(1447);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1446);
          match(PrestoSqlParser::NOT);
        }
        setState(1449);
        match(PrestoSqlParser::DISTINCT);
        setState(1450);
        match(PrestoSqlParser::FROM);
        setState(1451);
        antlrcpp::downCast<DistinctFromContext*>(_localctx)->right =
            valueExpression(0);
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ValueExpressionContext
//------------------------------------------------------------------

PrestoSqlParser::ValueExpressionContext::ValueExpressionContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::ValueExpressionContext::getRuleIndex() const {
  return PrestoSqlParser::RuleValueExpression;
}

void PrestoSqlParser::ValueExpressionContext::copyFrom(
    ValueExpressionContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ValueExpressionDefaultContext
//------------------------------------------------------------------

PrestoSqlParser::PrimaryExpressionContext*
PrestoSqlParser::ValueExpressionDefaultContext::primaryExpression() {
  return getRuleContext<PrestoSqlParser::PrimaryExpressionContext>(0);
}

PrestoSqlParser::ValueExpressionDefaultContext::ValueExpressionDefaultContext(
    ValueExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ValueExpressionDefaultContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterValueExpressionDefault(this);
}
void PrestoSqlParser::ValueExpressionDefaultContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitValueExpressionDefault(this);
}

antlrcpp::Any PrestoSqlParser::ValueExpressionDefaultContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitValueExpressionDefault(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ConcatenationContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ConcatenationContext::CONCAT() {
  return getToken(PrestoSqlParser::CONCAT, 0);
}

std::vector<PrestoSqlParser::ValueExpressionContext*>
PrestoSqlParser::ConcatenationContext::valueExpression() {
  return getRuleContexts<PrestoSqlParser::ValueExpressionContext>();
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::ConcatenationContext::valueExpression(size_t i) {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(i);
}

PrestoSqlParser::ConcatenationContext::ConcatenationContext(
    ValueExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ConcatenationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterConcatenation(this);
}
void PrestoSqlParser::ConcatenationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitConcatenation(this);
}

antlrcpp::Any PrestoSqlParser::ConcatenationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitConcatenation(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ArithmeticBinaryContext
//------------------------------------------------------------------

std::vector<PrestoSqlParser::ValueExpressionContext*>
PrestoSqlParser::ArithmeticBinaryContext::valueExpression() {
  return getRuleContexts<PrestoSqlParser::ValueExpressionContext>();
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::ArithmeticBinaryContext::valueExpression(size_t i) {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(i);
}

tree::TerminalNode* PrestoSqlParser::ArithmeticBinaryContext::ASTERISK() {
  return getToken(PrestoSqlParser::ASTERISK, 0);
}

tree::TerminalNode* PrestoSqlParser::ArithmeticBinaryContext::SLASH() {
  return getToken(PrestoSqlParser::SLASH, 0);
}

tree::TerminalNode* PrestoSqlParser::ArithmeticBinaryContext::PERCENT() {
  return getToken(PrestoSqlParser::PERCENT, 0);
}

tree::TerminalNode* PrestoSqlParser::ArithmeticBinaryContext::PLUS() {
  return getToken(PrestoSqlParser::PLUS, 0);
}

tree::TerminalNode* PrestoSqlParser::ArithmeticBinaryContext::MINUS() {
  return getToken(PrestoSqlParser::MINUS, 0);
}

PrestoSqlParser::ArithmeticBinaryContext::ArithmeticBinaryContext(
    ValueExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ArithmeticBinaryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterArithmeticBinary(this);
}
void PrestoSqlParser::ArithmeticBinaryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitArithmeticBinary(this);
}

antlrcpp::Any PrestoSqlParser::ArithmeticBinaryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitArithmeticBinary(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ArithmeticUnaryContext
//------------------------------------------------------------------

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::ArithmeticUnaryContext::valueExpression() {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ArithmeticUnaryContext::MINUS() {
  return getToken(PrestoSqlParser::MINUS, 0);
}

tree::TerminalNode* PrestoSqlParser::ArithmeticUnaryContext::PLUS() {
  return getToken(PrestoSqlParser::PLUS, 0);
}

PrestoSqlParser::ArithmeticUnaryContext::ArithmeticUnaryContext(
    ValueExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ArithmeticUnaryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterArithmeticUnary(this);
}
void PrestoSqlParser::ArithmeticUnaryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitArithmeticUnary(this);
}

antlrcpp::Any PrestoSqlParser::ArithmeticUnaryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitArithmeticUnary(this);
  else
    return visitor->visitChildren(this);
}
//----------------- AtTimeZoneContext
//------------------------------------------------------------------

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::AtTimeZoneContext::valueExpression() {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::AtTimeZoneContext::AT() {
  return getToken(PrestoSqlParser::AT, 0);
}

PrestoSqlParser::TimeZoneSpecifierContext*
PrestoSqlParser::AtTimeZoneContext::timeZoneSpecifier() {
  return getRuleContext<PrestoSqlParser::TimeZoneSpecifierContext>(0);
}

PrestoSqlParser::AtTimeZoneContext::AtTimeZoneContext(
    ValueExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::AtTimeZoneContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAtTimeZone(this);
}
void PrestoSqlParser::AtTimeZoneContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitAtTimeZone(this);
}

antlrcpp::Any PrestoSqlParser::AtTimeZoneContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAtTimeZone(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ValueExpressionContext* PrestoSqlParser::valueExpression() {
  return valueExpression(0);
}

PrestoSqlParser::ValueExpressionContext* PrestoSqlParser::valueExpression(
    int precedence) {
  ParserRuleContext* parentContext = _ctx;
  size_t parentState = getState();
  PrestoSqlParser::ValueExpressionContext* _localctx =
      _tracker.createInstance<ValueExpressionContext>(_ctx, parentState);
  PrestoSqlParser::ValueExpressionContext* previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by
                         // generated code.
  size_t startState = 90;
  enterRecursionRule(
      _localctx, 90, PrestoSqlParser::RuleValueExpression, precedence);

  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1458);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::T__1:
      case PrestoSqlParser::T__4:
      case PrestoSqlParser::ADD:
      case PrestoSqlParser::ADMIN:
      case PrestoSqlParser::ALL:
      case PrestoSqlParser::ANALYZE:
      case PrestoSqlParser::ANY:
      case PrestoSqlParser::ARRAY:
      case PrestoSqlParser::ASC:
      case PrestoSqlParser::AT:
      case PrestoSqlParser::BEFORE:
      case PrestoSqlParser::BERNOULLI:
      case PrestoSqlParser::CALL:
      case PrestoSqlParser::CALLED:
      case PrestoSqlParser::CASCADE:
      case PrestoSqlParser::CASE:
      case PrestoSqlParser::CAST:
      case PrestoSqlParser::CATALOGS:
      case PrestoSqlParser::COLUMN:
      case PrestoSqlParser::COLUMNS:
      case PrestoSqlParser::COMMENT:
      case PrestoSqlParser::COMMIT:
      case PrestoSqlParser::COMMITTED:
      case PrestoSqlParser::CURRENT:
      case PrestoSqlParser::CURRENT_DATE:
      case PrestoSqlParser::CURRENT_ROLE:
      case PrestoSqlParser::CURRENT_TIME:
      case PrestoSqlParser::CURRENT_TIMESTAMP:
      case PrestoSqlParser::CURRENT_USER:
      case PrestoSqlParser::DATA:
      case PrestoSqlParser::DATE:
      case PrestoSqlParser::DAY:
      case PrestoSqlParser::DEFINER:
      case PrestoSqlParser::DESC:
      case PrestoSqlParser::DETERMINISTIC:
      case PrestoSqlParser::DISABLED:
      case PrestoSqlParser::DISTRIBUTED:
      case PrestoSqlParser::ENABLED:
      case PrestoSqlParser::ENFORCED:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXISTS:
      case PrestoSqlParser::EXPLAIN:
      case PrestoSqlParser::EXTRACT:
      case PrestoSqlParser::EXTERNAL:
      case PrestoSqlParser::FALSE:
      case PrestoSqlParser::FETCH:
      case PrestoSqlParser::FILTER:
      case PrestoSqlParser::FIRST:
      case PrestoSqlParser::FOLLOWING:
      case PrestoSqlParser::FORMAT:
      case PrestoSqlParser::FUNCTION:
      case PrestoSqlParser::FUNCTIONS:
      case PrestoSqlParser::GRANT:
      case PrestoSqlParser::GRANTED:
      case PrestoSqlParser::GRANTS:
      case PrestoSqlParser::GRAPHVIZ:
      case PrestoSqlParser::GROUPING:
      case PrestoSqlParser::GROUPS:
      case PrestoSqlParser::HOUR:
      case PrestoSqlParser::IF:
      case PrestoSqlParser::IGNORE:
      case PrestoSqlParser::INCLUDING:
      case PrestoSqlParser::INPUT:
      case PrestoSqlParser::INTERVAL:
      case PrestoSqlParser::INVOKER:
      case PrestoSqlParser::IO:
      case PrestoSqlParser::ISOLATION:
      case PrestoSqlParser::JSON:
      case PrestoSqlParser::KEY:
      case PrestoSqlParser::LANGUAGE:
      case PrestoSqlParser::LAST:
      case PrestoSqlParser::LATERAL:
      case PrestoSqlParser::LEVEL:
      case PrestoSqlParser::LIMIT:
      case PrestoSqlParser::LOCALTIME:
      case PrestoSqlParser::LOCALTIMESTAMP:
      case PrestoSqlParser::LOGICAL:
      case PrestoSqlParser::MAP:
      case PrestoSqlParser::MATERIALIZED:
      case PrestoSqlParser::MINUTE:
      case PrestoSqlParser::MONTH:
      case PrestoSqlParser::NAME:
      case PrestoSqlParser::NFC:
      case PrestoSqlParser::NFD:
      case PrestoSqlParser::NFKC:
      case PrestoSqlParser::NFKD:
      case PrestoSqlParser::NO:
      case PrestoSqlParser::NONE:
      case PrestoSqlParser::NORMALIZE:
      case PrestoSqlParser::NULL_LITERAL:
      case PrestoSqlParser::NULLIF:
      case PrestoSqlParser::NULLS:
      case PrestoSqlParser::OF:
      case PrestoSqlParser::OFFSET:
      case PrestoSqlParser::ONLY:
      case PrestoSqlParser::OPTION:
      case PrestoSqlParser::ORDINALITY:
      case PrestoSqlParser::OUTPUT:
      case PrestoSqlParser::OVER:
      case PrestoSqlParser::PARTITION:
      case PrestoSqlParser::PARTITIONS:
      case PrestoSqlParser::POSITION:
      case PrestoSqlParser::PRECEDING:
      case PrestoSqlParser::PRIMARY:
      case PrestoSqlParser::PRIVILEGES:
      case PrestoSqlParser::PROPERTIES:
      case PrestoSqlParser::RANGE:
      case PrestoSqlParser::READ:
      case PrestoSqlParser::REFRESH:
      case PrestoSqlParser::RELY:
      case PrestoSqlParser::RENAME:
      case PrestoSqlParser::REPEATABLE:
      case PrestoSqlParser::REPLACE:
      case PrestoSqlParser::RESET:
      case PrestoSqlParser::RESPECT:
      case PrestoSqlParser::RESTRICT:
      case PrestoSqlParser::RETURN:
      case PrestoSqlParser::RETURNS:
      case PrestoSqlParser::REVOKE:
      case PrestoSqlParser::ROLE:
      case PrestoSqlParser::ROLES:
      case PrestoSqlParser::ROLLBACK:
      case PrestoSqlParser::ROW:
      case PrestoSqlParser::ROWS:
      case PrestoSqlParser::SCHEMA:
      case PrestoSqlParser::SCHEMAS:
      case PrestoSqlParser::SECOND:
      case PrestoSqlParser::SECURITY:
      case PrestoSqlParser::SERIALIZABLE:
      case PrestoSqlParser::SESSION:
      case PrestoSqlParser::SET:
      case PrestoSqlParser::SETS:
      case PrestoSqlParser::SHOW:
      case PrestoSqlParser::SOME:
      case PrestoSqlParser::SQL:
      case PrestoSqlParser::START:
      case PrestoSqlParser::STATS:
      case PrestoSqlParser::SUBSTRING:
      case PrestoSqlParser::SYSTEM:
      case PrestoSqlParser::SYSTEM_TIME:
      case PrestoSqlParser::SYSTEM_VERSION:
      case PrestoSqlParser::TABLES:
      case PrestoSqlParser::TABLESAMPLE:
      case PrestoSqlParser::TEMPORARY:
      case PrestoSqlParser::TEXT:
      case PrestoSqlParser::TIME:
      case PrestoSqlParser::TIMESTAMP:
      case PrestoSqlParser::TO:
      case PrestoSqlParser::TRANSACTION:
      case PrestoSqlParser::TRUE:
      case PrestoSqlParser::TRUNCATE:
      case PrestoSqlParser::TRY_CAST:
      case PrestoSqlParser::TYPE:
      case PrestoSqlParser::UNBOUNDED:
      case PrestoSqlParser::UNCOMMITTED:
      case PrestoSqlParser::UNIQUE:
      case PrestoSqlParser::UPDATE:
      case PrestoSqlParser::USE:
      case PrestoSqlParser::USER:
      case PrestoSqlParser::VALIDATE:
      case PrestoSqlParser::VERBOSE:
      case PrestoSqlParser::VERSION:
      case PrestoSqlParser::VIEW:
      case PrestoSqlParser::WORK:
      case PrestoSqlParser::WRITE:
      case PrestoSqlParser::YEAR:
      case PrestoSqlParser::ZONE:
      case PrestoSqlParser::STRING:
      case PrestoSqlParser::UNICODE_STRING:
      case PrestoSqlParser::BINARY_LITERAL:
      case PrestoSqlParser::INTEGER_VALUE:
      case PrestoSqlParser::DECIMAL_VALUE:
      case PrestoSqlParser::DOUBLE_VALUE:
      case PrestoSqlParser::IDENTIFIER:
      case PrestoSqlParser::DIGIT_IDENTIFIER:
      case PrestoSqlParser::QUOTED_IDENTIFIER:
      case PrestoSqlParser::BACKQUOTED_IDENTIFIER:
      case PrestoSqlParser::DOUBLE_PRECISION: {
        _localctx =
            _tracker.createInstance<ValueExpressionDefaultContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;

        setState(1455);
        primaryExpression(0);
        break;
      }

      case PrestoSqlParser::PLUS:
      case PrestoSqlParser::MINUS: {
        _localctx = _tracker.createInstance<ArithmeticUnaryContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1456);
        antlrcpp::downCast<ArithmeticUnaryContext*>(_localctx)->op =
            _input->LT(1);
        _la = _input->LA(1);
        if (!(_la == PrestoSqlParser::PLUS

              || _la == PrestoSqlParser::MINUS)) {
          antlrcpp::downCast<ArithmeticUnaryContext*>(_localctx)->op =
              _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(1457);
        valueExpression(4);
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    _ctx->stop = _input->LT(-1);
    setState(1474);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 189, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1472);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 188, _ctx)) {
          case 1: {
            auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(
                _tracker.createInstance<ValueExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->left = previousContext;
            pushNewRecursionContext(
                newContext, startState, RuleValueExpression);
            setState(1460);

            if (!(precpred(_ctx, 3)))
              throw FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(1461);
            antlrcpp::downCast<ArithmeticBinaryContext*>(_localctx)->op =
                _input->LT(1);
            _la = _input->LA(1);
            if (!(((((_la - 238) & ~0x3fULL) == 0) &&
                   ((1ULL << (_la - 238)) &
                    ((1ULL << (PrestoSqlParser::ASTERISK - 238)) |
                     (1ULL << (PrestoSqlParser::SLASH - 238)) |
                     (1ULL << (PrestoSqlParser::PERCENT - 238)))) != 0))) {
              antlrcpp::downCast<ArithmeticBinaryContext*>(_localctx)->op =
                  _errHandler->recoverInline(this);
            } else {
              _errHandler->reportMatch(this);
              consume();
            }
            setState(1462);
            antlrcpp::downCast<ArithmeticBinaryContext*>(_localctx)->right =
                valueExpression(4);
            break;
          }

          case 2: {
            auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(
                _tracker.createInstance<ValueExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->left = previousContext;
            pushNewRecursionContext(
                newContext, startState, RuleValueExpression);
            setState(1463);

            if (!(precpred(_ctx, 2)))
              throw FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(1464);
            antlrcpp::downCast<ArithmeticBinaryContext*>(_localctx)->op =
                _input->LT(1);
            _la = _input->LA(1);
            if (!(_la == PrestoSqlParser::PLUS

                  || _la == PrestoSqlParser::MINUS)) {
              antlrcpp::downCast<ArithmeticBinaryContext*>(_localctx)->op =
                  _errHandler->recoverInline(this);
            } else {
              _errHandler->reportMatch(this);
              consume();
            }
            setState(1465);
            antlrcpp::downCast<ArithmeticBinaryContext*>(_localctx)->right =
                valueExpression(3);
            break;
          }

          case 3: {
            auto newContext = _tracker.createInstance<ConcatenationContext>(
                _tracker.createInstance<ValueExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->left = previousContext;
            pushNewRecursionContext(
                newContext, startState, RuleValueExpression);
            setState(1466);

            if (!(precpred(_ctx, 1)))
              throw FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(1467);
            match(PrestoSqlParser::CONCAT);
            setState(1468);
            antlrcpp::downCast<ConcatenationContext*>(_localctx)->right =
                valueExpression(2);
            break;
          }

          case 4: {
            auto newContext = _tracker.createInstance<AtTimeZoneContext>(
                _tracker.createInstance<ValueExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            pushNewRecursionContext(
                newContext, startState, RuleValueExpression);
            setState(1469);

            if (!(precpred(_ctx, 5)))
              throw FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(1470);
            match(PrestoSqlParser::AT);
            setState(1471);
            timeZoneSpecifier();
            break;
          }

          default:
            break;
        }
      }
      setState(1476);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 189, _ctx);
    }
  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- PrimaryExpressionContext
//------------------------------------------------------------------

PrestoSqlParser::PrimaryExpressionContext::PrimaryExpressionContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::PrimaryExpressionContext::getRuleIndex() const {
  return PrestoSqlParser::RulePrimaryExpression;
}

void PrestoSqlParser::PrimaryExpressionContext::copyFrom(
    PrimaryExpressionContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- DereferenceContext
//------------------------------------------------------------------

PrestoSqlParser::PrimaryExpressionContext*
PrestoSqlParser::DereferenceContext::primaryExpression() {
  return getRuleContext<PrestoSqlParser::PrimaryExpressionContext>(0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::DereferenceContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::DereferenceContext::DereferenceContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DereferenceContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDereference(this);
}
void PrestoSqlParser::DereferenceContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDereference(this);
}

antlrcpp::Any PrestoSqlParser::DereferenceContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDereference(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TypeConstructorContext
//------------------------------------------------------------------

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::TypeConstructorContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::StringContext*
PrestoSqlParser::TypeConstructorContext::string() {
  return getRuleContext<PrestoSqlParser::StringContext>(0);
}

tree::TerminalNode*
PrestoSqlParser::TypeConstructorContext::DOUBLE_PRECISION() {
  return getToken(PrestoSqlParser::DOUBLE_PRECISION, 0);
}

PrestoSqlParser::TypeConstructorContext::TypeConstructorContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::TypeConstructorContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTypeConstructor(this);
}
void PrestoSqlParser::TypeConstructorContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTypeConstructor(this);
}

antlrcpp::Any PrestoSqlParser::TypeConstructorContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTypeConstructor(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SpecialDateTimeFunctionContext
//------------------------------------------------------------------

tree::TerminalNode*
PrestoSqlParser::SpecialDateTimeFunctionContext::CURRENT_DATE() {
  return getToken(PrestoSqlParser::CURRENT_DATE, 0);
}

tree::TerminalNode*
PrestoSqlParser::SpecialDateTimeFunctionContext::CURRENT_TIME() {
  return getToken(PrestoSqlParser::CURRENT_TIME, 0);
}

tree::TerminalNode*
PrestoSqlParser::SpecialDateTimeFunctionContext::INTEGER_VALUE() {
  return getToken(PrestoSqlParser::INTEGER_VALUE, 0);
}

tree::TerminalNode*
PrestoSqlParser::SpecialDateTimeFunctionContext::CURRENT_TIMESTAMP() {
  return getToken(PrestoSqlParser::CURRENT_TIMESTAMP, 0);
}

tree::TerminalNode*
PrestoSqlParser::SpecialDateTimeFunctionContext::LOCALTIME() {
  return getToken(PrestoSqlParser::LOCALTIME, 0);
}

tree::TerminalNode*
PrestoSqlParser::SpecialDateTimeFunctionContext::LOCALTIMESTAMP() {
  return getToken(PrestoSqlParser::LOCALTIMESTAMP, 0);
}

PrestoSqlParser::SpecialDateTimeFunctionContext::SpecialDateTimeFunctionContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SpecialDateTimeFunctionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSpecialDateTimeFunction(this);
}
void PrestoSqlParser::SpecialDateTimeFunctionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSpecialDateTimeFunction(this);
}

antlrcpp::Any PrestoSqlParser::SpecialDateTimeFunctionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSpecialDateTimeFunction(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SubstringContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::SubstringContext::SUBSTRING() {
  return getToken(PrestoSqlParser::SUBSTRING, 0);
}

std::vector<PrestoSqlParser::ValueExpressionContext*>
PrestoSqlParser::SubstringContext::valueExpression() {
  return getRuleContexts<PrestoSqlParser::ValueExpressionContext>();
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::SubstringContext::valueExpression(size_t i) {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(i);
}

tree::TerminalNode* PrestoSqlParser::SubstringContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

tree::TerminalNode* PrestoSqlParser::SubstringContext::FOR() {
  return getToken(PrestoSqlParser::FOR, 0);
}

PrestoSqlParser::SubstringContext::SubstringContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SubstringContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSubstring(this);
}
void PrestoSqlParser::SubstringContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSubstring(this);
}

antlrcpp::Any PrestoSqlParser::SubstringContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSubstring(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CastContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CastContext::CAST() {
  return getToken(PrestoSqlParser::CAST, 0);
}

PrestoSqlParser::ExpressionContext* PrestoSqlParser::CastContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CastContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

PrestoSqlParser::TypeContext* PrestoSqlParser::CastContext::type() {
  return getRuleContext<PrestoSqlParser::TypeContext>(0);
}

tree::TerminalNode* PrestoSqlParser::CastContext::TRY_CAST() {
  return getToken(PrestoSqlParser::TRY_CAST, 0);
}

PrestoSqlParser::CastContext::CastContext(PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CastContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCast(this);
}
void PrestoSqlParser::CastContext::exitRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCast(this);
}

antlrcpp::Any PrestoSqlParser::CastContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCast(this);
  else
    return visitor->visitChildren(this);
}
//----------------- LambdaContext
//------------------------------------------------------------------

std::vector<PrestoSqlParser::IdentifierContext*>
PrestoSqlParser::LambdaContext::identifier() {
  return getRuleContexts<PrestoSqlParser::IdentifierContext>();
}

PrestoSqlParser::IdentifierContext* PrestoSqlParser::LambdaContext::identifier(
    size_t i) {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(i);
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::LambdaContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

PrestoSqlParser::LambdaContext::LambdaContext(PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::LambdaContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterLambda(this);
}
void PrestoSqlParser::LambdaContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitLambda(this);
}

antlrcpp::Any PrestoSqlParser::LambdaContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitLambda(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ParenthesizedExpressionContext
//------------------------------------------------------------------

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::ParenthesizedExpressionContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

PrestoSqlParser::ParenthesizedExpressionContext::ParenthesizedExpressionContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ParenthesizedExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterParenthesizedExpression(this);
}
void PrestoSqlParser::ParenthesizedExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitParenthesizedExpression(this);
}

antlrcpp::Any PrestoSqlParser::ParenthesizedExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitParenthesizedExpression(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ParameterContext
//------------------------------------------------------------------

PrestoSqlParser::ParameterContext::ParameterContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ParameterContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterParameter(this);
}
void PrestoSqlParser::ParameterContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitParameter(this);
}

antlrcpp::Any PrestoSqlParser::ParameterContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitParameter(this);
  else
    return visitor->visitChildren(this);
}
//----------------- NormalizeContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::NormalizeContext::NORMALIZE() {
  return getToken(PrestoSqlParser::NORMALIZE, 0);
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::NormalizeContext::valueExpression() {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(0);
}

PrestoSqlParser::NormalFormContext*
PrestoSqlParser::NormalizeContext::normalForm() {
  return getRuleContext<PrestoSqlParser::NormalFormContext>(0);
}

PrestoSqlParser::NormalizeContext::NormalizeContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::NormalizeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNormalize(this);
}
void PrestoSqlParser::NormalizeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNormalize(this);
}

antlrcpp::Any PrestoSqlParser::NormalizeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNormalize(this);
  else
    return visitor->visitChildren(this);
}
//----------------- IntervalLiteralContext
//------------------------------------------------------------------

PrestoSqlParser::IntervalContext*
PrestoSqlParser::IntervalLiteralContext::interval() {
  return getRuleContext<PrestoSqlParser::IntervalContext>(0);
}

PrestoSqlParser::IntervalLiteralContext::IntervalLiteralContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::IntervalLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterIntervalLiteral(this);
}
void PrestoSqlParser::IntervalLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitIntervalLiteral(this);
}

antlrcpp::Any PrestoSqlParser::IntervalLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitIntervalLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- NumericLiteralContext
//------------------------------------------------------------------

PrestoSqlParser::NumberContext*
PrestoSqlParser::NumericLiteralContext::number() {
  return getRuleContext<PrestoSqlParser::NumberContext>(0);
}

PrestoSqlParser::NumericLiteralContext::NumericLiteralContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::NumericLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteral(this);
}
void PrestoSqlParser::NumericLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteral(this);
}

antlrcpp::Any PrestoSqlParser::NumericLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNumericLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- BooleanLiteralContext
//------------------------------------------------------------------

PrestoSqlParser::BooleanValueContext*
PrestoSqlParser::BooleanLiteralContext::booleanValue() {
  return getRuleContext<PrestoSqlParser::BooleanValueContext>(0);
}

PrestoSqlParser::BooleanLiteralContext::BooleanLiteralContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::BooleanLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBooleanLiteral(this);
}
void PrestoSqlParser::BooleanLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitBooleanLiteral(this);
}

antlrcpp::Any PrestoSqlParser::BooleanLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBooleanLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SimpleCaseContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::SimpleCaseContext::CASE() {
  return getToken(PrestoSqlParser::CASE, 0);
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::SimpleCaseContext::valueExpression() {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::SimpleCaseContext::END() {
  return getToken(PrestoSqlParser::END, 0);
}

std::vector<PrestoSqlParser::WhenClauseContext*>
PrestoSqlParser::SimpleCaseContext::whenClause() {
  return getRuleContexts<PrestoSqlParser::WhenClauseContext>();
}

PrestoSqlParser::WhenClauseContext*
PrestoSqlParser::SimpleCaseContext::whenClause(size_t i) {
  return getRuleContext<PrestoSqlParser::WhenClauseContext>(i);
}

tree::TerminalNode* PrestoSqlParser::SimpleCaseContext::ELSE() {
  return getToken(PrestoSqlParser::ELSE, 0);
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::SimpleCaseContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

PrestoSqlParser::SimpleCaseContext::SimpleCaseContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SimpleCaseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSimpleCase(this);
}
void PrestoSqlParser::SimpleCaseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSimpleCase(this);
}

antlrcpp::Any PrestoSqlParser::SimpleCaseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSimpleCase(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ColumnReferenceContext
//------------------------------------------------------------------

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ColumnReferenceContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::ColumnReferenceContext::ColumnReferenceContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ColumnReferenceContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterColumnReference(this);
}
void PrestoSqlParser::ColumnReferenceContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitColumnReference(this);
}

antlrcpp::Any PrestoSqlParser::ColumnReferenceContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitColumnReference(this);
  else
    return visitor->visitChildren(this);
}
//----------------- NullLiteralContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::NullLiteralContext::NULL_LITERAL() {
  return getToken(PrestoSqlParser::NULL_LITERAL, 0);
}

PrestoSqlParser::NullLiteralContext::NullLiteralContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::NullLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNullLiteral(this);
}
void PrestoSqlParser::NullLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNullLiteral(this);
}

antlrcpp::Any PrestoSqlParser::NullLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNullLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RowConstructorContext
//------------------------------------------------------------------

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::RowConstructorContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::RowConstructorContext::expression(size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

tree::TerminalNode* PrestoSqlParser::RowConstructorContext::ROW() {
  return getToken(PrestoSqlParser::ROW, 0);
}

PrestoSqlParser::RowConstructorContext::RowConstructorContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RowConstructorContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRowConstructor(this);
}
void PrestoSqlParser::RowConstructorContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRowConstructor(this);
}

antlrcpp::Any PrestoSqlParser::RowConstructorContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRowConstructor(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SubscriptContext
//------------------------------------------------------------------

PrestoSqlParser::PrimaryExpressionContext*
PrestoSqlParser::SubscriptContext::primaryExpression() {
  return getRuleContext<PrestoSqlParser::PrimaryExpressionContext>(0);
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::SubscriptContext::valueExpression() {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(0);
}

PrestoSqlParser::SubscriptContext::SubscriptContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SubscriptContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSubscript(this);
}
void PrestoSqlParser::SubscriptContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSubscript(this);
}

antlrcpp::Any PrestoSqlParser::SubscriptContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSubscript(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SubqueryExpressionContext
//------------------------------------------------------------------

PrestoSqlParser::QueryContext*
PrestoSqlParser::SubqueryExpressionContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

PrestoSqlParser::SubqueryExpressionContext::SubqueryExpressionContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SubqueryExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSubqueryExpression(this);
}
void PrestoSqlParser::SubqueryExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSubqueryExpression(this);
}

antlrcpp::Any PrestoSqlParser::SubqueryExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSubqueryExpression(this);
  else
    return visitor->visitChildren(this);
}
//----------------- BinaryLiteralContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::BinaryLiteralContext::BINARY_LITERAL() {
  return getToken(PrestoSqlParser::BINARY_LITERAL, 0);
}

PrestoSqlParser::BinaryLiteralContext::BinaryLiteralContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::BinaryLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBinaryLiteral(this);
}
void PrestoSqlParser::BinaryLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitBinaryLiteral(this);
}

antlrcpp::Any PrestoSqlParser::BinaryLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBinaryLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CurrentUserContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CurrentUserContext::CURRENT_USER() {
  return getToken(PrestoSqlParser::CURRENT_USER, 0);
}

PrestoSqlParser::CurrentUserContext::CurrentUserContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CurrentUserContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCurrentUser(this);
}
void PrestoSqlParser::CurrentUserContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCurrentUser(this);
}

antlrcpp::Any PrestoSqlParser::CurrentUserContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCurrentUser(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ExtractContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ExtractContext::EXTRACT() {
  return getToken(PrestoSqlParser::EXTRACT, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ExtractContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ExtractContext::FROM() {
  return getToken(PrestoSqlParser::FROM, 0);
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::ExtractContext::valueExpression() {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(0);
}

PrestoSqlParser::ExtractContext::ExtractContext(PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ExtractContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterExtract(this);
}
void PrestoSqlParser::ExtractContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitExtract(this);
}

antlrcpp::Any PrestoSqlParser::ExtractContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExtract(this);
  else
    return visitor->visitChildren(this);
}
//----------------- StringLiteralContext
//------------------------------------------------------------------

PrestoSqlParser::StringContext*
PrestoSqlParser::StringLiteralContext::string() {
  return getRuleContext<PrestoSqlParser::StringContext>(0);
}

PrestoSqlParser::StringLiteralContext::StringLiteralContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::StringLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterStringLiteral(this);
}
void PrestoSqlParser::StringLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitStringLiteral(this);
}

antlrcpp::Any PrestoSqlParser::StringLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitStringLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ArrayConstructorContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ArrayConstructorContext::ARRAY() {
  return getToken(PrestoSqlParser::ARRAY, 0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::ArrayConstructorContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::ArrayConstructorContext::expression(size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

PrestoSqlParser::ArrayConstructorContext::ArrayConstructorContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ArrayConstructorContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterArrayConstructor(this);
}
void PrestoSqlParser::ArrayConstructorContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitArrayConstructor(this);
}

antlrcpp::Any PrestoSqlParser::ArrayConstructorContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitArrayConstructor(this);
  else
    return visitor->visitChildren(this);
}
//----------------- FunctionCallContext
//------------------------------------------------------------------

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::FunctionCallContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::FunctionCallContext::ASTERISK() {
  return getToken(PrestoSqlParser::ASTERISK, 0);
}

PrestoSqlParser::FilterContext* PrestoSqlParser::FunctionCallContext::filter() {
  return getRuleContext<PrestoSqlParser::FilterContext>(0);
}

PrestoSqlParser::OverContext* PrestoSqlParser::FunctionCallContext::over() {
  return getRuleContext<PrestoSqlParser::OverContext>(0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::FunctionCallContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::FunctionCallContext::expression(size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

tree::TerminalNode* PrestoSqlParser::FunctionCallContext::ORDER() {
  return getToken(PrestoSqlParser::ORDER, 0);
}

tree::TerminalNode* PrestoSqlParser::FunctionCallContext::BY() {
  return getToken(PrestoSqlParser::BY, 0);
}

std::vector<PrestoSqlParser::SortItemContext*>
PrestoSqlParser::FunctionCallContext::sortItem() {
  return getRuleContexts<PrestoSqlParser::SortItemContext>();
}

PrestoSqlParser::SortItemContext*
PrestoSqlParser::FunctionCallContext::sortItem(size_t i) {
  return getRuleContext<PrestoSqlParser::SortItemContext>(i);
}

PrestoSqlParser::SetQuantifierContext*
PrestoSqlParser::FunctionCallContext::setQuantifier() {
  return getRuleContext<PrestoSqlParser::SetQuantifierContext>(0);
}

PrestoSqlParser::NullTreatmentContext*
PrestoSqlParser::FunctionCallContext::nullTreatment() {
  return getRuleContext<PrestoSqlParser::NullTreatmentContext>(0);
}

PrestoSqlParser::FunctionCallContext::FunctionCallContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::FunctionCallContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterFunctionCall(this);
}
void PrestoSqlParser::FunctionCallContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitFunctionCall(this);
}

antlrcpp::Any PrestoSqlParser::FunctionCallContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitFunctionCall(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ExistsContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ExistsContext::EXISTS() {
  return getToken(PrestoSqlParser::EXISTS, 0);
}

PrestoSqlParser::QueryContext* PrestoSqlParser::ExistsContext::query() {
  return getRuleContext<PrestoSqlParser::QueryContext>(0);
}

PrestoSqlParser::ExistsContext::ExistsContext(PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ExistsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterExists(this);
}
void PrestoSqlParser::ExistsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitExists(this);
}

antlrcpp::Any PrestoSqlParser::ExistsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExists(this);
  else
    return visitor->visitChildren(this);
}
//----------------- PositionContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::PositionContext::POSITION() {
  return getToken(PrestoSqlParser::POSITION, 0);
}

std::vector<PrestoSqlParser::ValueExpressionContext*>
PrestoSqlParser::PositionContext::valueExpression() {
  return getRuleContexts<PrestoSqlParser::ValueExpressionContext>();
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::PositionContext::valueExpression(size_t i) {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(i);
}

tree::TerminalNode* PrestoSqlParser::PositionContext::IN() {
  return getToken(PrestoSqlParser::IN, 0);
}

PrestoSqlParser::PositionContext::PositionContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::PositionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterPosition(this);
}
void PrestoSqlParser::PositionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitPosition(this);
}

antlrcpp::Any PrestoSqlParser::PositionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitPosition(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SearchedCaseContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::SearchedCaseContext::CASE() {
  return getToken(PrestoSqlParser::CASE, 0);
}

tree::TerminalNode* PrestoSqlParser::SearchedCaseContext::END() {
  return getToken(PrestoSqlParser::END, 0);
}

std::vector<PrestoSqlParser::WhenClauseContext*>
PrestoSqlParser::SearchedCaseContext::whenClause() {
  return getRuleContexts<PrestoSqlParser::WhenClauseContext>();
}

PrestoSqlParser::WhenClauseContext*
PrestoSqlParser::SearchedCaseContext::whenClause(size_t i) {
  return getRuleContext<PrestoSqlParser::WhenClauseContext>(i);
}

tree::TerminalNode* PrestoSqlParser::SearchedCaseContext::ELSE() {
  return getToken(PrestoSqlParser::ELSE, 0);
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::SearchedCaseContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

PrestoSqlParser::SearchedCaseContext::SearchedCaseContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SearchedCaseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSearchedCase(this);
}
void PrestoSqlParser::SearchedCaseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSearchedCase(this);
}

antlrcpp::Any PrestoSqlParser::SearchedCaseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSearchedCase(this);
  else
    return visitor->visitChildren(this);
}
//----------------- GroupingOperationContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::GroupingOperationContext::GROUPING() {
  return getToken(PrestoSqlParser::GROUPING, 0);
}

std::vector<PrestoSqlParser::QualifiedNameContext*>
PrestoSqlParser::GroupingOperationContext::qualifiedName() {
  return getRuleContexts<PrestoSqlParser::QualifiedNameContext>();
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::GroupingOperationContext::qualifiedName(size_t i) {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(i);
}

PrestoSqlParser::GroupingOperationContext::GroupingOperationContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::GroupingOperationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupingOperation(this);
}
void PrestoSqlParser::GroupingOperationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupingOperation(this);
}

antlrcpp::Any PrestoSqlParser::GroupingOperationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitGroupingOperation(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::PrimaryExpressionContext*
PrestoSqlParser::primaryExpression() {
  return primaryExpression(0);
}

PrestoSqlParser::PrimaryExpressionContext* PrestoSqlParser::primaryExpression(
    int precedence) {
  ParserRuleContext* parentContext = _ctx;
  size_t parentState = getState();
  PrestoSqlParser::PrimaryExpressionContext* _localctx =
      _tracker.createInstance<PrimaryExpressionContext>(_ctx, parentState);
  PrestoSqlParser::PrimaryExpressionContext* previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by
                         // generated code.
  size_t startState = 92;
  enterRecursionRule(
      _localctx, 92, PrestoSqlParser::RulePrimaryExpression, precedence);

  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1716);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 218, _ctx)) {
      case 1: {
        _localctx = _tracker.createInstance<NullLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;

        setState(1478);
        match(PrestoSqlParser::NULL_LITERAL);
        break;
      }

      case 2: {
        _localctx = _tracker.createInstance<IntervalLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1479);
        interval();
        break;
      }

      case 3: {
        _localctx = _tracker.createInstance<TypeConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1480);
        identifier();
        setState(1481);
        string();
        break;
      }

      case 4: {
        _localctx = _tracker.createInstance<TypeConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1483);
        match(PrestoSqlParser::DOUBLE_PRECISION);
        setState(1484);
        string();
        break;
      }

      case 5: {
        _localctx = _tracker.createInstance<NumericLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1485);
        number();
        break;
      }

      case 6: {
        _localctx = _tracker.createInstance<BooleanLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1486);
        booleanValue();
        break;
      }

      case 7: {
        _localctx = _tracker.createInstance<StringLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1487);
        string();
        break;
      }

      case 8: {
        _localctx = _tracker.createInstance<BinaryLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1488);
        match(PrestoSqlParser::BINARY_LITERAL);
        break;
      }

      case 9: {
        _localctx = _tracker.createInstance<ParameterContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1489);
        match(PrestoSqlParser::T__4);
        break;
      }

      case 10: {
        _localctx = _tracker.createInstance<PositionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1490);
        match(PrestoSqlParser::POSITION);
        setState(1491);
        match(PrestoSqlParser::T__1);
        setState(1492);
        valueExpression(0);
        setState(1493);
        match(PrestoSqlParser::IN);
        setState(1494);
        valueExpression(0);
        setState(1495);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 11: {
        _localctx = _tracker.createInstance<RowConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1497);
        match(PrestoSqlParser::T__1);
        setState(1498);
        expression();
        setState(1501);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(1499);
          match(PrestoSqlParser::T__3);
          setState(1500);
          expression();
          setState(1503);
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == PrestoSqlParser::T__3);
        setState(1505);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 12: {
        _localctx = _tracker.createInstance<RowConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1507);
        match(PrestoSqlParser::ROW);
        setState(1508);
        match(PrestoSqlParser::T__1);
        setState(1509);
        expression();
        setState(1514);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1510);
          match(PrestoSqlParser::T__3);
          setState(1511);
          expression();
          setState(1516);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1517);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 13: {
        _localctx = _tracker.createInstance<FunctionCallContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1519);
        qualifiedName();
        setState(1520);
        match(PrestoSqlParser::T__1);
        setState(1521);
        match(PrestoSqlParser::ASTERISK);
        setState(1522);
        match(PrestoSqlParser::T__2);
        setState(1524);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 192, _ctx)) {
          case 1: {
            setState(1523);
            filter();
            break;
          }

          default:
            break;
        }
        setState(1527);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 193, _ctx)) {
          case 1: {
            setState(1526);
            over();
            break;
          }

          default:
            break;
        }
        break;
      }

      case 14: {
        _localctx = _tracker.createInstance<FunctionCallContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1529);
        qualifiedName();
        setState(1530);
        match(PrestoSqlParser::T__1);
        setState(1542);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) &
              ((1ULL << PrestoSqlParser::T__1) |
               (1ULL << PrestoSqlParser::T__4) |
               (1ULL << PrestoSqlParser::ADD) |
               (1ULL << PrestoSqlParser::ADMIN) |
               (1ULL << PrestoSqlParser::ALL) |
               (1ULL << PrestoSqlParser::ANALYZE) |
               (1ULL << PrestoSqlParser::ANY) |
               (1ULL << PrestoSqlParser::ARRAY) |
               (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
               (1ULL << PrestoSqlParser::BEFORE) |
               (1ULL << PrestoSqlParser::BERNOULLI) |
               (1ULL << PrestoSqlParser::CALL) |
               (1ULL << PrestoSqlParser::CALLED) |
               (1ULL << PrestoSqlParser::CASCADE) |
               (1ULL << PrestoSqlParser::CASE) |
               (1ULL << PrestoSqlParser::CAST) |
               (1ULL << PrestoSqlParser::CATALOGS) |
               (1ULL << PrestoSqlParser::COLUMN) |
               (1ULL << PrestoSqlParser::COLUMNS) |
               (1ULL << PrestoSqlParser::COMMENT) |
               (1ULL << PrestoSqlParser::COMMIT) |
               (1ULL << PrestoSqlParser::COMMITTED) |
               (1ULL << PrestoSqlParser::CURRENT) |
               (1ULL << PrestoSqlParser::CURRENT_DATE) |
               (1ULL << PrestoSqlParser::CURRENT_ROLE) |
               (1ULL << PrestoSqlParser::CURRENT_TIME) |
               (1ULL << PrestoSqlParser::CURRENT_TIMESTAMP) |
               (1ULL << PrestoSqlParser::CURRENT_USER) |
               (1ULL << PrestoSqlParser::DATA) |
               (1ULL << PrestoSqlParser::DATE) |
               (1ULL << PrestoSqlParser::DAY) |
               (1ULL << PrestoSqlParser::DEFINER) |
               (1ULL << PrestoSqlParser::DESC) |
               (1ULL << PrestoSqlParser::DETERMINISTIC) |
               (1ULL << PrestoSqlParser::DISABLED) |
               (1ULL << PrestoSqlParser::DISTINCT) |
               (1ULL << PrestoSqlParser::DISTRIBUTED) |
               (1ULL << PrestoSqlParser::ENABLED) |
               (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) &
              ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::EXISTS - 66)) |
               (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
               (1ULL << (PrestoSqlParser::EXTRACT - 66)) |
               (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
               (1ULL << (PrestoSqlParser::FALSE - 66)) |
               (1ULL << (PrestoSqlParser::FETCH - 66)) |
               (1ULL << (PrestoSqlParser::FILTER - 66)) |
               (1ULL << (PrestoSqlParser::FIRST - 66)) |
               (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
               (1ULL << (PrestoSqlParser::FORMAT - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
               (1ULL << (PrestoSqlParser::GRANT - 66)) |
               (1ULL << (PrestoSqlParser::GRANTED - 66)) |
               (1ULL << (PrestoSqlParser::GRANTS - 66)) |
               (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
               (1ULL << (PrestoSqlParser::GROUPING - 66)) |
               (1ULL << (PrestoSqlParser::GROUPS - 66)) |
               (1ULL << (PrestoSqlParser::HOUR - 66)) |
               (1ULL << (PrestoSqlParser::IF - 66)) |
               (1ULL << (PrestoSqlParser::IGNORE - 66)) |
               (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::INPUT - 66)) |
               (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
               (1ULL << (PrestoSqlParser::INVOKER - 66)) |
               (1ULL << (PrestoSqlParser::IO - 66)) |
               (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
               (1ULL << (PrestoSqlParser::JSON - 66)) |
               (1ULL << (PrestoSqlParser::KEY - 66)) |
               (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
               (1ULL << (PrestoSqlParser::LAST - 66)) |
               (1ULL << (PrestoSqlParser::LATERAL - 66)) |
               (1ULL << (PrestoSqlParser::LEVEL - 66)) |
               (1ULL << (PrestoSqlParser::LIMIT - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIME - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIMESTAMP - 66)) |
               (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
               (1ULL << (PrestoSqlParser::MAP - 66)) |
               (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
               (1ULL << (PrestoSqlParser::MINUTE - 66)) |
               (1ULL << (PrestoSqlParser::MONTH - 66)) |
               (1ULL << (PrestoSqlParser::NAME - 66)) |
               (1ULL << (PrestoSqlParser::NFC - 66)) |
               (1ULL << (PrestoSqlParser::NFD - 66)) |
               (1ULL << (PrestoSqlParser::NFKC - 66)) |
               (1ULL << (PrestoSqlParser::NFKD - 66)) |
               (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) &
              ((1ULL << (PrestoSqlParser::NONE - 130)) |
               (1ULL << (PrestoSqlParser::NORMALIZE - 130)) |
               (1ULL << (PrestoSqlParser::NOT - 130)) |
               (1ULL << (PrestoSqlParser::NULL_LITERAL - 130)) |
               (1ULL << (PrestoSqlParser::NULLIF - 130)) |
               (1ULL << (PrestoSqlParser::NULLS - 130)) |
               (1ULL << (PrestoSqlParser::OF - 130)) |
               (1ULL << (PrestoSqlParser::OFFSET - 130)) |
               (1ULL << (PrestoSqlParser::ONLY - 130)) |
               (1ULL << (PrestoSqlParser::OPTION - 130)) |
               (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
               (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
               (1ULL << (PrestoSqlParser::OVER - 130)) |
               (1ULL << (PrestoSqlParser::PARTITION - 130)) |
               (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
               (1ULL << (PrestoSqlParser::POSITION - 130)) |
               (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
               (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
               (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
               (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
               (1ULL << (PrestoSqlParser::RANGE - 130)) |
               (1ULL << (PrestoSqlParser::READ - 130)) |
               (1ULL << (PrestoSqlParser::REFRESH - 130)) |
               (1ULL << (PrestoSqlParser::RELY - 130)) |
               (1ULL << (PrestoSqlParser::RENAME - 130)) |
               (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
               (1ULL << (PrestoSqlParser::REPLACE - 130)) |
               (1ULL << (PrestoSqlParser::RESET - 130)) |
               (1ULL << (PrestoSqlParser::RESPECT - 130)) |
               (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
               (1ULL << (PrestoSqlParser::RETURN - 130)) |
               (1ULL << (PrestoSqlParser::RETURNS - 130)) |
               (1ULL << (PrestoSqlParser::REVOKE - 130)) |
               (1ULL << (PrestoSqlParser::ROLE - 130)) |
               (1ULL << (PrestoSqlParser::ROLES - 130)) |
               (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
               (1ULL << (PrestoSqlParser::ROW - 130)) |
               (1ULL << (PrestoSqlParser::ROWS - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
               (1ULL << (PrestoSqlParser::SECOND - 130)) |
               (1ULL << (PrestoSqlParser::SECURITY - 130)) |
               (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
               (1ULL << (PrestoSqlParser::SESSION - 130)) |
               (1ULL << (PrestoSqlParser::SET - 130)) |
               (1ULL << (PrestoSqlParser::SETS - 130)) |
               (1ULL << (PrestoSqlParser::SHOW - 130)) |
               (1ULL << (PrestoSqlParser::SOME - 130)) |
               (1ULL << (PrestoSqlParser::SQL - 130)) |
               (1ULL << (PrestoSqlParser::START - 130)) |
               (1ULL << (PrestoSqlParser::STATS - 130)) |
               (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
            ((((_la - 195) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 195)) &
              ((1ULL << (PrestoSqlParser::TABLES - 195)) |
               (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
               (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
               (1ULL << (PrestoSqlParser::TEXT - 195)) |
               (1ULL << (PrestoSqlParser::TIME - 195)) |
               (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
               (1ULL << (PrestoSqlParser::TO - 195)) |
               (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
               (1ULL << (PrestoSqlParser::TRUE - 195)) |
               (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
               (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
               (1ULL << (PrestoSqlParser::TYPE - 195)) |
               (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
               (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
               (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
               (1ULL << (PrestoSqlParser::UPDATE - 195)) |
               (1ULL << (PrestoSqlParser::USE - 195)) |
               (1ULL << (PrestoSqlParser::USER - 195)) |
               (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
               (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
               (1ULL << (PrestoSqlParser::VERSION - 195)) |
               (1ULL << (PrestoSqlParser::VIEW - 195)) |
               (1ULL << (PrestoSqlParser::WORK - 195)) |
               (1ULL << (PrestoSqlParser::WRITE - 195)) |
               (1ULL << (PrestoSqlParser::YEAR - 195)) |
               (1ULL << (PrestoSqlParser::ZONE - 195)) |
               (1ULL << (PrestoSqlParser::PLUS - 195)) |
               (1ULL << (PrestoSqlParser::MINUS - 195)) |
               (1ULL << (PrestoSqlParser::STRING - 195)) |
               (1ULL << (PrestoSqlParser::UNICODE_STRING - 195)) |
               (1ULL << (PrestoSqlParser::BINARY_LITERAL - 195)) |
               (1ULL << (PrestoSqlParser::INTEGER_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DECIMAL_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DIGIT_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::QUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::BACKQUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_PRECISION - 195)))) != 0)) {
          setState(1532);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 194, _ctx)) {
            case 1: {
              setState(1531);
              setQuantifier();
              break;
            }

            default:
              break;
          }
          setState(1534);
          expression();
          setState(1539);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1535);
            match(PrestoSqlParser::T__3);
            setState(1536);
            expression();
            setState(1541);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1554);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::ORDER) {
          setState(1544);
          match(PrestoSqlParser::ORDER);
          setState(1545);
          match(PrestoSqlParser::BY);
          setState(1546);
          sortItem();
          setState(1551);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1547);
            match(PrestoSqlParser::T__3);
            setState(1548);
            sortItem();
            setState(1553);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1556);
        match(PrestoSqlParser::T__2);
        setState(1558);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 199, _ctx)) {
          case 1: {
            setState(1557);
            filter();
            break;
          }

          default:
            break;
        }
        setState(1564);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 201, _ctx)) {
          case 1: {
            setState(1561);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == PrestoSqlParser::IGNORE ||
                _la == PrestoSqlParser::RESPECT) {
              setState(1560);
              nullTreatment();
            }
            setState(1563);
            over();
            break;
          }

          default:
            break;
        }
        break;
      }

      case 15: {
        _localctx = _tracker.createInstance<LambdaContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1566);
        identifier();
        setState(1567);
        match(PrestoSqlParser::T__5);
        setState(1568);
        expression();
        break;
      }

      case 16: {
        _localctx = _tracker.createInstance<LambdaContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1570);
        match(PrestoSqlParser::T__1);
        setState(1579);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) &
              ((1ULL << PrestoSqlParser::ADD) |
               (1ULL << PrestoSqlParser::ADMIN) |
               (1ULL << PrestoSqlParser::ALL) |
               (1ULL << PrestoSqlParser::ANALYZE) |
               (1ULL << PrestoSqlParser::ANY) |
               (1ULL << PrestoSqlParser::ARRAY) |
               (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
               (1ULL << PrestoSqlParser::BEFORE) |
               (1ULL << PrestoSqlParser::BERNOULLI) |
               (1ULL << PrestoSqlParser::CALL) |
               (1ULL << PrestoSqlParser::CALLED) |
               (1ULL << PrestoSqlParser::CASCADE) |
               (1ULL << PrestoSqlParser::CATALOGS) |
               (1ULL << PrestoSqlParser::COLUMN) |
               (1ULL << PrestoSqlParser::COLUMNS) |
               (1ULL << PrestoSqlParser::COMMENT) |
               (1ULL << PrestoSqlParser::COMMIT) |
               (1ULL << PrestoSqlParser::COMMITTED) |
               (1ULL << PrestoSqlParser::CURRENT) |
               (1ULL << PrestoSqlParser::CURRENT_ROLE) |
               (1ULL << PrestoSqlParser::DATA) |
               (1ULL << PrestoSqlParser::DATE) |
               (1ULL << PrestoSqlParser::DAY) |
               (1ULL << PrestoSqlParser::DEFINER) |
               (1ULL << PrestoSqlParser::DESC) |
               (1ULL << PrestoSqlParser::DETERMINISTIC) |
               (1ULL << PrestoSqlParser::DISABLED) |
               (1ULL << PrestoSqlParser::DISTRIBUTED) |
               (1ULL << PrestoSqlParser::ENABLED) |
               (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) &
              ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
               (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
               (1ULL << (PrestoSqlParser::FETCH - 66)) |
               (1ULL << (PrestoSqlParser::FILTER - 66)) |
               (1ULL << (PrestoSqlParser::FIRST - 66)) |
               (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
               (1ULL << (PrestoSqlParser::FORMAT - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
               (1ULL << (PrestoSqlParser::GRANT - 66)) |
               (1ULL << (PrestoSqlParser::GRANTED - 66)) |
               (1ULL << (PrestoSqlParser::GRANTS - 66)) |
               (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
               (1ULL << (PrestoSqlParser::GROUPS - 66)) |
               (1ULL << (PrestoSqlParser::HOUR - 66)) |
               (1ULL << (PrestoSqlParser::IF - 66)) |
               (1ULL << (PrestoSqlParser::IGNORE - 66)) |
               (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::INPUT - 66)) |
               (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
               (1ULL << (PrestoSqlParser::INVOKER - 66)) |
               (1ULL << (PrestoSqlParser::IO - 66)) |
               (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
               (1ULL << (PrestoSqlParser::JSON - 66)) |
               (1ULL << (PrestoSqlParser::KEY - 66)) |
               (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
               (1ULL << (PrestoSqlParser::LAST - 66)) |
               (1ULL << (PrestoSqlParser::LATERAL - 66)) |
               (1ULL << (PrestoSqlParser::LEVEL - 66)) |
               (1ULL << (PrestoSqlParser::LIMIT - 66)) |
               (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
               (1ULL << (PrestoSqlParser::MAP - 66)) |
               (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
               (1ULL << (PrestoSqlParser::MINUTE - 66)) |
               (1ULL << (PrestoSqlParser::MONTH - 66)) |
               (1ULL << (PrestoSqlParser::NAME - 66)) |
               (1ULL << (PrestoSqlParser::NFC - 66)) |
               (1ULL << (PrestoSqlParser::NFD - 66)) |
               (1ULL << (PrestoSqlParser::NFKC - 66)) |
               (1ULL << (PrestoSqlParser::NFKD - 66)) |
               (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) &
              ((1ULL << (PrestoSqlParser::NONE - 130)) |
               (1ULL << (PrestoSqlParser::NULLIF - 130)) |
               (1ULL << (PrestoSqlParser::NULLS - 130)) |
               (1ULL << (PrestoSqlParser::OF - 130)) |
               (1ULL << (PrestoSqlParser::OFFSET - 130)) |
               (1ULL << (PrestoSqlParser::ONLY - 130)) |
               (1ULL << (PrestoSqlParser::OPTION - 130)) |
               (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
               (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
               (1ULL << (PrestoSqlParser::OVER - 130)) |
               (1ULL << (PrestoSqlParser::PARTITION - 130)) |
               (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
               (1ULL << (PrestoSqlParser::POSITION - 130)) |
               (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
               (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
               (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
               (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
               (1ULL << (PrestoSqlParser::RANGE - 130)) |
               (1ULL << (PrestoSqlParser::READ - 130)) |
               (1ULL << (PrestoSqlParser::REFRESH - 130)) |
               (1ULL << (PrestoSqlParser::RELY - 130)) |
               (1ULL << (PrestoSqlParser::RENAME - 130)) |
               (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
               (1ULL << (PrestoSqlParser::REPLACE - 130)) |
               (1ULL << (PrestoSqlParser::RESET - 130)) |
               (1ULL << (PrestoSqlParser::RESPECT - 130)) |
               (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
               (1ULL << (PrestoSqlParser::RETURN - 130)) |
               (1ULL << (PrestoSqlParser::RETURNS - 130)) |
               (1ULL << (PrestoSqlParser::REVOKE - 130)) |
               (1ULL << (PrestoSqlParser::ROLE - 130)) |
               (1ULL << (PrestoSqlParser::ROLES - 130)) |
               (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
               (1ULL << (PrestoSqlParser::ROW - 130)) |
               (1ULL << (PrestoSqlParser::ROWS - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
               (1ULL << (PrestoSqlParser::SECOND - 130)) |
               (1ULL << (PrestoSqlParser::SECURITY - 130)) |
               (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
               (1ULL << (PrestoSqlParser::SESSION - 130)) |
               (1ULL << (PrestoSqlParser::SET - 130)) |
               (1ULL << (PrestoSqlParser::SETS - 130)) |
               (1ULL << (PrestoSqlParser::SHOW - 130)) |
               (1ULL << (PrestoSqlParser::SOME - 130)) |
               (1ULL << (PrestoSqlParser::SQL - 130)) |
               (1ULL << (PrestoSqlParser::START - 130)) |
               (1ULL << (PrestoSqlParser::STATS - 130)) |
               (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
            ((((_la - 195) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 195)) &
              ((1ULL << (PrestoSqlParser::TABLES - 195)) |
               (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
               (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
               (1ULL << (PrestoSqlParser::TEXT - 195)) |
               (1ULL << (PrestoSqlParser::TIME - 195)) |
               (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
               (1ULL << (PrestoSqlParser::TO - 195)) |
               (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
               (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
               (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
               (1ULL << (PrestoSqlParser::TYPE - 195)) |
               (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
               (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
               (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
               (1ULL << (PrestoSqlParser::UPDATE - 195)) |
               (1ULL << (PrestoSqlParser::USE - 195)) |
               (1ULL << (PrestoSqlParser::USER - 195)) |
               (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
               (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
               (1ULL << (PrestoSqlParser::VERSION - 195)) |
               (1ULL << (PrestoSqlParser::VIEW - 195)) |
               (1ULL << (PrestoSqlParser::WORK - 195)) |
               (1ULL << (PrestoSqlParser::WRITE - 195)) |
               (1ULL << (PrestoSqlParser::YEAR - 195)) |
               (1ULL << (PrestoSqlParser::ZONE - 195)) |
               (1ULL << (PrestoSqlParser::IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DIGIT_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::QUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::BACKQUOTED_IDENTIFIER - 195)))) !=
                 0)) {
          setState(1571);
          identifier();
          setState(1576);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1572);
            match(PrestoSqlParser::T__3);
            setState(1573);
            identifier();
            setState(1578);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1581);
        match(PrestoSqlParser::T__2);
        setState(1582);
        match(PrestoSqlParser::T__5);
        setState(1583);
        expression();
        break;
      }

      case 17: {
        _localctx =
            _tracker.createInstance<SubqueryExpressionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1584);
        match(PrestoSqlParser::T__1);
        setState(1585);
        query();
        setState(1586);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 18: {
        _localctx = _tracker.createInstance<ExistsContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1588);
        match(PrestoSqlParser::EXISTS);
        setState(1589);
        match(PrestoSqlParser::T__1);
        setState(1590);
        query();
        setState(1591);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 19: {
        _localctx = _tracker.createInstance<SimpleCaseContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1593);
        match(PrestoSqlParser::CASE);
        setState(1594);
        valueExpression(0);
        setState(1596);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(1595);
          whenClause();
          setState(1598);
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == PrestoSqlParser::WHEN);
        setState(1602);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::ELSE) {
          setState(1600);
          match(PrestoSqlParser::ELSE);
          setState(1601);
          antlrcpp::downCast<SimpleCaseContext*>(_localctx)->elseExpression =
              expression();
        }
        setState(1604);
        match(PrestoSqlParser::END);
        break;
      }

      case 20: {
        _localctx = _tracker.createInstance<SearchedCaseContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1606);
        match(PrestoSqlParser::CASE);
        setState(1608);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(1607);
          whenClause();
          setState(1610);
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == PrestoSqlParser::WHEN);
        setState(1614);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::ELSE) {
          setState(1612);
          match(PrestoSqlParser::ELSE);
          setState(1613);
          antlrcpp::downCast<SearchedCaseContext*>(_localctx)->elseExpression =
              expression();
        }
        setState(1616);
        match(PrestoSqlParser::END);
        break;
      }

      case 21: {
        _localctx = _tracker.createInstance<CastContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1618);
        match(PrestoSqlParser::CAST);
        setState(1619);
        match(PrestoSqlParser::T__1);
        setState(1620);
        expression();
        setState(1621);
        match(PrestoSqlParser::AS);
        setState(1622);
        type(0);
        setState(1623);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 22: {
        _localctx = _tracker.createInstance<CastContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1625);
        match(PrestoSqlParser::TRY_CAST);
        setState(1626);
        match(PrestoSqlParser::T__1);
        setState(1627);
        expression();
        setState(1628);
        match(PrestoSqlParser::AS);
        setState(1629);
        type(0);
        setState(1630);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 23: {
        _localctx = _tracker.createInstance<ArrayConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1632);
        match(PrestoSqlParser::ARRAY);
        setState(1633);
        match(PrestoSqlParser::T__6);
        setState(1642);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) &
              ((1ULL << PrestoSqlParser::T__1) |
               (1ULL << PrestoSqlParser::T__4) |
               (1ULL << PrestoSqlParser::ADD) |
               (1ULL << PrestoSqlParser::ADMIN) |
               (1ULL << PrestoSqlParser::ALL) |
               (1ULL << PrestoSqlParser::ANALYZE) |
               (1ULL << PrestoSqlParser::ANY) |
               (1ULL << PrestoSqlParser::ARRAY) |
               (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
               (1ULL << PrestoSqlParser::BEFORE) |
               (1ULL << PrestoSqlParser::BERNOULLI) |
               (1ULL << PrestoSqlParser::CALL) |
               (1ULL << PrestoSqlParser::CALLED) |
               (1ULL << PrestoSqlParser::CASCADE) |
               (1ULL << PrestoSqlParser::CASE) |
               (1ULL << PrestoSqlParser::CAST) |
               (1ULL << PrestoSqlParser::CATALOGS) |
               (1ULL << PrestoSqlParser::COLUMN) |
               (1ULL << PrestoSqlParser::COLUMNS) |
               (1ULL << PrestoSqlParser::COMMENT) |
               (1ULL << PrestoSqlParser::COMMIT) |
               (1ULL << PrestoSqlParser::COMMITTED) |
               (1ULL << PrestoSqlParser::CURRENT) |
               (1ULL << PrestoSqlParser::CURRENT_DATE) |
               (1ULL << PrestoSqlParser::CURRENT_ROLE) |
               (1ULL << PrestoSqlParser::CURRENT_TIME) |
               (1ULL << PrestoSqlParser::CURRENT_TIMESTAMP) |
               (1ULL << PrestoSqlParser::CURRENT_USER) |
               (1ULL << PrestoSqlParser::DATA) |
               (1ULL << PrestoSqlParser::DATE) |
               (1ULL << PrestoSqlParser::DAY) |
               (1ULL << PrestoSqlParser::DEFINER) |
               (1ULL << PrestoSqlParser::DESC) |
               (1ULL << PrestoSqlParser::DETERMINISTIC) |
               (1ULL << PrestoSqlParser::DISABLED) |
               (1ULL << PrestoSqlParser::DISTRIBUTED) |
               (1ULL << PrestoSqlParser::ENABLED) |
               (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) &
              ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::EXISTS - 66)) |
               (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
               (1ULL << (PrestoSqlParser::EXTRACT - 66)) |
               (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
               (1ULL << (PrestoSqlParser::FALSE - 66)) |
               (1ULL << (PrestoSqlParser::FETCH - 66)) |
               (1ULL << (PrestoSqlParser::FILTER - 66)) |
               (1ULL << (PrestoSqlParser::FIRST - 66)) |
               (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
               (1ULL << (PrestoSqlParser::FORMAT - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
               (1ULL << (PrestoSqlParser::GRANT - 66)) |
               (1ULL << (PrestoSqlParser::GRANTED - 66)) |
               (1ULL << (PrestoSqlParser::GRANTS - 66)) |
               (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
               (1ULL << (PrestoSqlParser::GROUPING - 66)) |
               (1ULL << (PrestoSqlParser::GROUPS - 66)) |
               (1ULL << (PrestoSqlParser::HOUR - 66)) |
               (1ULL << (PrestoSqlParser::IF - 66)) |
               (1ULL << (PrestoSqlParser::IGNORE - 66)) |
               (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::INPUT - 66)) |
               (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
               (1ULL << (PrestoSqlParser::INVOKER - 66)) |
               (1ULL << (PrestoSqlParser::IO - 66)) |
               (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
               (1ULL << (PrestoSqlParser::JSON - 66)) |
               (1ULL << (PrestoSqlParser::KEY - 66)) |
               (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
               (1ULL << (PrestoSqlParser::LAST - 66)) |
               (1ULL << (PrestoSqlParser::LATERAL - 66)) |
               (1ULL << (PrestoSqlParser::LEVEL - 66)) |
               (1ULL << (PrestoSqlParser::LIMIT - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIME - 66)) |
               (1ULL << (PrestoSqlParser::LOCALTIMESTAMP - 66)) |
               (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
               (1ULL << (PrestoSqlParser::MAP - 66)) |
               (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
               (1ULL << (PrestoSqlParser::MINUTE - 66)) |
               (1ULL << (PrestoSqlParser::MONTH - 66)) |
               (1ULL << (PrestoSqlParser::NAME - 66)) |
               (1ULL << (PrestoSqlParser::NFC - 66)) |
               (1ULL << (PrestoSqlParser::NFD - 66)) |
               (1ULL << (PrestoSqlParser::NFKC - 66)) |
               (1ULL << (PrestoSqlParser::NFKD - 66)) |
               (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) &
              ((1ULL << (PrestoSqlParser::NONE - 130)) |
               (1ULL << (PrestoSqlParser::NORMALIZE - 130)) |
               (1ULL << (PrestoSqlParser::NOT - 130)) |
               (1ULL << (PrestoSqlParser::NULL_LITERAL - 130)) |
               (1ULL << (PrestoSqlParser::NULLIF - 130)) |
               (1ULL << (PrestoSqlParser::NULLS - 130)) |
               (1ULL << (PrestoSqlParser::OF - 130)) |
               (1ULL << (PrestoSqlParser::OFFSET - 130)) |
               (1ULL << (PrestoSqlParser::ONLY - 130)) |
               (1ULL << (PrestoSqlParser::OPTION - 130)) |
               (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
               (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
               (1ULL << (PrestoSqlParser::OVER - 130)) |
               (1ULL << (PrestoSqlParser::PARTITION - 130)) |
               (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
               (1ULL << (PrestoSqlParser::POSITION - 130)) |
               (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
               (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
               (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
               (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
               (1ULL << (PrestoSqlParser::RANGE - 130)) |
               (1ULL << (PrestoSqlParser::READ - 130)) |
               (1ULL << (PrestoSqlParser::REFRESH - 130)) |
               (1ULL << (PrestoSqlParser::RELY - 130)) |
               (1ULL << (PrestoSqlParser::RENAME - 130)) |
               (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
               (1ULL << (PrestoSqlParser::REPLACE - 130)) |
               (1ULL << (PrestoSqlParser::RESET - 130)) |
               (1ULL << (PrestoSqlParser::RESPECT - 130)) |
               (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
               (1ULL << (PrestoSqlParser::RETURN - 130)) |
               (1ULL << (PrestoSqlParser::RETURNS - 130)) |
               (1ULL << (PrestoSqlParser::REVOKE - 130)) |
               (1ULL << (PrestoSqlParser::ROLE - 130)) |
               (1ULL << (PrestoSqlParser::ROLES - 130)) |
               (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
               (1ULL << (PrestoSqlParser::ROW - 130)) |
               (1ULL << (PrestoSqlParser::ROWS - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
               (1ULL << (PrestoSqlParser::SECOND - 130)) |
               (1ULL << (PrestoSqlParser::SECURITY - 130)) |
               (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
               (1ULL << (PrestoSqlParser::SESSION - 130)) |
               (1ULL << (PrestoSqlParser::SET - 130)) |
               (1ULL << (PrestoSqlParser::SETS - 130)) |
               (1ULL << (PrestoSqlParser::SHOW - 130)) |
               (1ULL << (PrestoSqlParser::SOME - 130)) |
               (1ULL << (PrestoSqlParser::SQL - 130)) |
               (1ULL << (PrestoSqlParser::START - 130)) |
               (1ULL << (PrestoSqlParser::STATS - 130)) |
               (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
            ((((_la - 195) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 195)) &
              ((1ULL << (PrestoSqlParser::TABLES - 195)) |
               (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
               (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
               (1ULL << (PrestoSqlParser::TEXT - 195)) |
               (1ULL << (PrestoSqlParser::TIME - 195)) |
               (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
               (1ULL << (PrestoSqlParser::TO - 195)) |
               (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
               (1ULL << (PrestoSqlParser::TRUE - 195)) |
               (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
               (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
               (1ULL << (PrestoSqlParser::TYPE - 195)) |
               (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
               (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
               (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
               (1ULL << (PrestoSqlParser::UPDATE - 195)) |
               (1ULL << (PrestoSqlParser::USE - 195)) |
               (1ULL << (PrestoSqlParser::USER - 195)) |
               (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
               (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
               (1ULL << (PrestoSqlParser::VERSION - 195)) |
               (1ULL << (PrestoSqlParser::VIEW - 195)) |
               (1ULL << (PrestoSqlParser::WORK - 195)) |
               (1ULL << (PrestoSqlParser::WRITE - 195)) |
               (1ULL << (PrestoSqlParser::YEAR - 195)) |
               (1ULL << (PrestoSqlParser::ZONE - 195)) |
               (1ULL << (PrestoSqlParser::PLUS - 195)) |
               (1ULL << (PrestoSqlParser::MINUS - 195)) |
               (1ULL << (PrestoSqlParser::STRING - 195)) |
               (1ULL << (PrestoSqlParser::UNICODE_STRING - 195)) |
               (1ULL << (PrestoSqlParser::BINARY_LITERAL - 195)) |
               (1ULL << (PrestoSqlParser::INTEGER_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DECIMAL_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_VALUE - 195)) |
               (1ULL << (PrestoSqlParser::IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DIGIT_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::QUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::BACKQUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DOUBLE_PRECISION - 195)))) != 0)) {
          setState(1634);
          expression();
          setState(1639);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1635);
            match(PrestoSqlParser::T__3);
            setState(1636);
            expression();
            setState(1641);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1644);
        match(PrestoSqlParser::T__7);
        break;
      }

      case 24: {
        _localctx = _tracker.createInstance<ColumnReferenceContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1645);
        identifier();
        break;
      }

      case 25: {
        _localctx =
            _tracker.createInstance<SpecialDateTimeFunctionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1646);
        antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)->name =
            match(PrestoSqlParser::CURRENT_DATE);
        break;
      }

      case 26: {
        _localctx =
            _tracker.createInstance<SpecialDateTimeFunctionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1647);
        antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)->name =
            match(PrestoSqlParser::CURRENT_TIME);
        setState(1651);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 210, _ctx)) {
          case 1: {
            setState(1648);
            match(PrestoSqlParser::T__1);
            setState(1649);
            antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)
                ->precision = match(PrestoSqlParser::INTEGER_VALUE);
            setState(1650);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 27: {
        _localctx =
            _tracker.createInstance<SpecialDateTimeFunctionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1653);
        antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)->name =
            match(PrestoSqlParser::CURRENT_TIMESTAMP);
        setState(1657);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 211, _ctx)) {
          case 1: {
            setState(1654);
            match(PrestoSqlParser::T__1);
            setState(1655);
            antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)
                ->precision = match(PrestoSqlParser::INTEGER_VALUE);
            setState(1656);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 28: {
        _localctx =
            _tracker.createInstance<SpecialDateTimeFunctionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1659);
        antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)->name =
            match(PrestoSqlParser::LOCALTIME);
        setState(1663);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 212, _ctx)) {
          case 1: {
            setState(1660);
            match(PrestoSqlParser::T__1);
            setState(1661);
            antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)
                ->precision = match(PrestoSqlParser::INTEGER_VALUE);
            setState(1662);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 29: {
        _localctx =
            _tracker.createInstance<SpecialDateTimeFunctionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1665);
        antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)->name =
            match(PrestoSqlParser::LOCALTIMESTAMP);
        setState(1669);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 213, _ctx)) {
          case 1: {
            setState(1666);
            match(PrestoSqlParser::T__1);
            setState(1667);
            antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)
                ->precision = match(PrestoSqlParser::INTEGER_VALUE);
            setState(1668);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 30: {
        _localctx = _tracker.createInstance<CurrentUserContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1671);
        antlrcpp::downCast<CurrentUserContext*>(_localctx)->name =
            match(PrestoSqlParser::CURRENT_USER);
        break;
      }

      case 31: {
        _localctx = _tracker.createInstance<SubstringContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1672);
        match(PrestoSqlParser::SUBSTRING);
        setState(1673);
        match(PrestoSqlParser::T__1);
        setState(1674);
        valueExpression(0);
        setState(1675);
        match(PrestoSqlParser::FROM);
        setState(1676);
        valueExpression(0);
        setState(1679);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::FOR) {
          setState(1677);
          match(PrestoSqlParser::FOR);
          setState(1678);
          valueExpression(0);
        }
        setState(1681);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 32: {
        _localctx = _tracker.createInstance<NormalizeContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1683);
        match(PrestoSqlParser::NORMALIZE);
        setState(1684);
        match(PrestoSqlParser::T__1);
        setState(1685);
        valueExpression(0);
        setState(1688);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::T__3) {
          setState(1686);
          match(PrestoSqlParser::T__3);
          setState(1687);
          normalForm();
        }
        setState(1690);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 33: {
        _localctx = _tracker.createInstance<ExtractContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1692);
        match(PrestoSqlParser::EXTRACT);
        setState(1693);
        match(PrestoSqlParser::T__1);
        setState(1694);
        identifier();
        setState(1695);
        match(PrestoSqlParser::FROM);
        setState(1696);
        valueExpression(0);
        setState(1697);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 34: {
        _localctx =
            _tracker.createInstance<ParenthesizedExpressionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1699);
        match(PrestoSqlParser::T__1);
        setState(1700);
        expression();
        setState(1701);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 35: {
        _localctx =
            _tracker.createInstance<GroupingOperationContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1703);
        match(PrestoSqlParser::GROUPING);
        setState(1704);
        match(PrestoSqlParser::T__1);
        setState(1713);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) &
              ((1ULL << PrestoSqlParser::ADD) |
               (1ULL << PrestoSqlParser::ADMIN) |
               (1ULL << PrestoSqlParser::ALL) |
               (1ULL << PrestoSqlParser::ANALYZE) |
               (1ULL << PrestoSqlParser::ANY) |
               (1ULL << PrestoSqlParser::ARRAY) |
               (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
               (1ULL << PrestoSqlParser::BEFORE) |
               (1ULL << PrestoSqlParser::BERNOULLI) |
               (1ULL << PrestoSqlParser::CALL) |
               (1ULL << PrestoSqlParser::CALLED) |
               (1ULL << PrestoSqlParser::CASCADE) |
               (1ULL << PrestoSqlParser::CATALOGS) |
               (1ULL << PrestoSqlParser::COLUMN) |
               (1ULL << PrestoSqlParser::COLUMNS) |
               (1ULL << PrestoSqlParser::COMMENT) |
               (1ULL << PrestoSqlParser::COMMIT) |
               (1ULL << PrestoSqlParser::COMMITTED) |
               (1ULL << PrestoSqlParser::CURRENT) |
               (1ULL << PrestoSqlParser::CURRENT_ROLE) |
               (1ULL << PrestoSqlParser::DATA) |
               (1ULL << PrestoSqlParser::DATE) |
               (1ULL << PrestoSqlParser::DAY) |
               (1ULL << PrestoSqlParser::DEFINER) |
               (1ULL << PrestoSqlParser::DESC) |
               (1ULL << PrestoSqlParser::DETERMINISTIC) |
               (1ULL << PrestoSqlParser::DISABLED) |
               (1ULL << PrestoSqlParser::DISTRIBUTED) |
               (1ULL << PrestoSqlParser::ENABLED) |
               (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) &
              ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
               (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
               (1ULL << (PrestoSqlParser::FETCH - 66)) |
               (1ULL << (PrestoSqlParser::FILTER - 66)) |
               (1ULL << (PrestoSqlParser::FIRST - 66)) |
               (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
               (1ULL << (PrestoSqlParser::FORMAT - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
               (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
               (1ULL << (PrestoSqlParser::GRANT - 66)) |
               (1ULL << (PrestoSqlParser::GRANTED - 66)) |
               (1ULL << (PrestoSqlParser::GRANTS - 66)) |
               (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
               (1ULL << (PrestoSqlParser::GROUPS - 66)) |
               (1ULL << (PrestoSqlParser::HOUR - 66)) |
               (1ULL << (PrestoSqlParser::IF - 66)) |
               (1ULL << (PrestoSqlParser::IGNORE - 66)) |
               (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
               (1ULL << (PrestoSqlParser::INPUT - 66)) |
               (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
               (1ULL << (PrestoSqlParser::INVOKER - 66)) |
               (1ULL << (PrestoSqlParser::IO - 66)) |
               (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
               (1ULL << (PrestoSqlParser::JSON - 66)) |
               (1ULL << (PrestoSqlParser::KEY - 66)) |
               (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
               (1ULL << (PrestoSqlParser::LAST - 66)) |
               (1ULL << (PrestoSqlParser::LATERAL - 66)) |
               (1ULL << (PrestoSqlParser::LEVEL - 66)) |
               (1ULL << (PrestoSqlParser::LIMIT - 66)) |
               (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
               (1ULL << (PrestoSqlParser::MAP - 66)) |
               (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
               (1ULL << (PrestoSqlParser::MINUTE - 66)) |
               (1ULL << (PrestoSqlParser::MONTH - 66)) |
               (1ULL << (PrestoSqlParser::NAME - 66)) |
               (1ULL << (PrestoSqlParser::NFC - 66)) |
               (1ULL << (PrestoSqlParser::NFD - 66)) |
               (1ULL << (PrestoSqlParser::NFKC - 66)) |
               (1ULL << (PrestoSqlParser::NFKD - 66)) |
               (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) &
              ((1ULL << (PrestoSqlParser::NONE - 130)) |
               (1ULL << (PrestoSqlParser::NULLIF - 130)) |
               (1ULL << (PrestoSqlParser::NULLS - 130)) |
               (1ULL << (PrestoSqlParser::OF - 130)) |
               (1ULL << (PrestoSqlParser::OFFSET - 130)) |
               (1ULL << (PrestoSqlParser::ONLY - 130)) |
               (1ULL << (PrestoSqlParser::OPTION - 130)) |
               (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
               (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
               (1ULL << (PrestoSqlParser::OVER - 130)) |
               (1ULL << (PrestoSqlParser::PARTITION - 130)) |
               (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
               (1ULL << (PrestoSqlParser::POSITION - 130)) |
               (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
               (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
               (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
               (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
               (1ULL << (PrestoSqlParser::RANGE - 130)) |
               (1ULL << (PrestoSqlParser::READ - 130)) |
               (1ULL << (PrestoSqlParser::REFRESH - 130)) |
               (1ULL << (PrestoSqlParser::RELY - 130)) |
               (1ULL << (PrestoSqlParser::RENAME - 130)) |
               (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
               (1ULL << (PrestoSqlParser::REPLACE - 130)) |
               (1ULL << (PrestoSqlParser::RESET - 130)) |
               (1ULL << (PrestoSqlParser::RESPECT - 130)) |
               (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
               (1ULL << (PrestoSqlParser::RETURN - 130)) |
               (1ULL << (PrestoSqlParser::RETURNS - 130)) |
               (1ULL << (PrestoSqlParser::REVOKE - 130)) |
               (1ULL << (PrestoSqlParser::ROLE - 130)) |
               (1ULL << (PrestoSqlParser::ROLES - 130)) |
               (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
               (1ULL << (PrestoSqlParser::ROW - 130)) |
               (1ULL << (PrestoSqlParser::ROWS - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
               (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
               (1ULL << (PrestoSqlParser::SECOND - 130)) |
               (1ULL << (PrestoSqlParser::SECURITY - 130)) |
               (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
               (1ULL << (PrestoSqlParser::SESSION - 130)) |
               (1ULL << (PrestoSqlParser::SET - 130)) |
               (1ULL << (PrestoSqlParser::SETS - 130)) |
               (1ULL << (PrestoSqlParser::SHOW - 130)) |
               (1ULL << (PrestoSqlParser::SOME - 130)) |
               (1ULL << (PrestoSqlParser::SQL - 130)) |
               (1ULL << (PrestoSqlParser::START - 130)) |
               (1ULL << (PrestoSqlParser::STATS - 130)) |
               (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
               (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
            ((((_la - 195) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 195)) &
              ((1ULL << (PrestoSqlParser::TABLES - 195)) |
               (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
               (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
               (1ULL << (PrestoSqlParser::TEXT - 195)) |
               (1ULL << (PrestoSqlParser::TIME - 195)) |
               (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
               (1ULL << (PrestoSqlParser::TO - 195)) |
               (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
               (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
               (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
               (1ULL << (PrestoSqlParser::TYPE - 195)) |
               (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
               (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
               (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
               (1ULL << (PrestoSqlParser::UPDATE - 195)) |
               (1ULL << (PrestoSqlParser::USE - 195)) |
               (1ULL << (PrestoSqlParser::USER - 195)) |
               (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
               (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
               (1ULL << (PrestoSqlParser::VERSION - 195)) |
               (1ULL << (PrestoSqlParser::VIEW - 195)) |
               (1ULL << (PrestoSqlParser::WORK - 195)) |
               (1ULL << (PrestoSqlParser::WRITE - 195)) |
               (1ULL << (PrestoSqlParser::YEAR - 195)) |
               (1ULL << (PrestoSqlParser::ZONE - 195)) |
               (1ULL << (PrestoSqlParser::IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::DIGIT_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::QUOTED_IDENTIFIER - 195)) |
               (1ULL << (PrestoSqlParser::BACKQUOTED_IDENTIFIER - 195)))) !=
                 0)) {
          setState(1705);
          qualifiedName();
          setState(1710);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1706);
            match(PrestoSqlParser::T__3);
            setState(1707);
            qualifiedName();
            setState(1712);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1715);
        match(PrestoSqlParser::T__2);
        break;
      }

      default:
        break;
    }
    _ctx->stop = _input->LT(-1);
    setState(1728);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 220, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1726);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 219, _ctx)) {
          case 1: {
            auto newContext = _tracker.createInstance<SubscriptContext>(
                _tracker.createInstance<PrimaryExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->value = previousContext;
            pushNewRecursionContext(
                newContext, startState, RulePrimaryExpression);
            setState(1718);

            if (!(precpred(_ctx, 14)))
              throw FailedPredicateException(this, "precpred(_ctx, 14)");
            setState(1719);
            match(PrestoSqlParser::T__6);
            setState(1720);
            antlrcpp::downCast<SubscriptContext*>(_localctx)->index =
                valueExpression(0);
            setState(1721);
            match(PrestoSqlParser::T__7);
            break;
          }

          case 2: {
            auto newContext = _tracker.createInstance<DereferenceContext>(
                _tracker.createInstance<PrimaryExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->base = previousContext;
            pushNewRecursionContext(
                newContext, startState, RulePrimaryExpression);
            setState(1723);

            if (!(precpred(_ctx, 12)))
              throw FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(1724);
            match(PrestoSqlParser::T__0);
            setState(1725);
            antlrcpp::downCast<DereferenceContext*>(_localctx)->fieldName =
                identifier();
            break;
          }

          default:
            break;
        }
      }
      setState(1730);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 220, _ctx);
    }
  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- StringContext
//------------------------------------------------------------------

PrestoSqlParser::StringContext::StringContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::StringContext::getRuleIndex() const {
  return PrestoSqlParser::RuleString;
}

void PrestoSqlParser::StringContext::copyFrom(StringContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- UnicodeStringLiteralContext
//------------------------------------------------------------------

tree::TerminalNode*
PrestoSqlParser::UnicodeStringLiteralContext::UNICODE_STRING() {
  return getToken(PrestoSqlParser::UNICODE_STRING, 0);
}

tree::TerminalNode* PrestoSqlParser::UnicodeStringLiteralContext::UESCAPE() {
  return getToken(PrestoSqlParser::UESCAPE, 0);
}

tree::TerminalNode* PrestoSqlParser::UnicodeStringLiteralContext::STRING() {
  return getToken(PrestoSqlParser::STRING, 0);
}

PrestoSqlParser::UnicodeStringLiteralContext::UnicodeStringLiteralContext(
    StringContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::UnicodeStringLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnicodeStringLiteral(this);
}
void PrestoSqlParser::UnicodeStringLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnicodeStringLiteral(this);
}

antlrcpp::Any PrestoSqlParser::UnicodeStringLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUnicodeStringLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- BasicStringLiteralContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::BasicStringLiteralContext::STRING() {
  return getToken(PrestoSqlParser::STRING, 0);
}

PrestoSqlParser::BasicStringLiteralContext::BasicStringLiteralContext(
    StringContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::BasicStringLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBasicStringLiteral(this);
}
void PrestoSqlParser::BasicStringLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitBasicStringLiteral(this);
}

antlrcpp::Any PrestoSqlParser::BasicStringLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBasicStringLiteral(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::StringContext* PrestoSqlParser::string() {
  StringContext* _localctx =
      _tracker.createInstance<StringContext>(_ctx, getState());
  enterRule(_localctx, 94, PrestoSqlParser::RuleString);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1737);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::STRING: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::BasicStringLiteralContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1731);
        match(PrestoSqlParser::STRING);
        break;
      }

      case PrestoSqlParser::UNICODE_STRING: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::UnicodeStringLiteralContext>(
                    _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1732);
        match(PrestoSqlParser::UNICODE_STRING);
        setState(1735);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 221, _ctx)) {
          case 1: {
            setState(1733);
            match(PrestoSqlParser::UESCAPE);
            setState(1734);
            match(PrestoSqlParser::STRING);
            break;
          }

          default:
            break;
        }
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NullTreatmentContext
//------------------------------------------------------------------

PrestoSqlParser::NullTreatmentContext::NullTreatmentContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::NullTreatmentContext::IGNORE() {
  return getToken(PrestoSqlParser::IGNORE, 0);
}

tree::TerminalNode* PrestoSqlParser::NullTreatmentContext::NULLS() {
  return getToken(PrestoSqlParser::NULLS, 0);
}

tree::TerminalNode* PrestoSqlParser::NullTreatmentContext::RESPECT() {
  return getToken(PrestoSqlParser::RESPECT, 0);
}

size_t PrestoSqlParser::NullTreatmentContext::getRuleIndex() const {
  return PrestoSqlParser::RuleNullTreatment;
}

void PrestoSqlParser::NullTreatmentContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNullTreatment(this);
}

void PrestoSqlParser::NullTreatmentContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNullTreatment(this);
}

antlrcpp::Any PrestoSqlParser::NullTreatmentContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNullTreatment(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::NullTreatmentContext* PrestoSqlParser::nullTreatment() {
  NullTreatmentContext* _localctx =
      _tracker.createInstance<NullTreatmentContext>(_ctx, getState());
  enterRule(_localctx, 96, PrestoSqlParser::RuleNullTreatment);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1743);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::IGNORE: {
        enterOuterAlt(_localctx, 1);
        setState(1739);
        match(PrestoSqlParser::IGNORE);
        setState(1740);
        match(PrestoSqlParser::NULLS);
        break;
      }

      case PrestoSqlParser::RESPECT: {
        enterOuterAlt(_localctx, 2);
        setState(1741);
        match(PrestoSqlParser::RESPECT);
        setState(1742);
        match(PrestoSqlParser::NULLS);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TimeZoneSpecifierContext
//------------------------------------------------------------------

PrestoSqlParser::TimeZoneSpecifierContext::TimeZoneSpecifierContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::TimeZoneSpecifierContext::getRuleIndex() const {
  return PrestoSqlParser::RuleTimeZoneSpecifier;
}

void PrestoSqlParser::TimeZoneSpecifierContext::copyFrom(
    TimeZoneSpecifierContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- TimeZoneIntervalContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::TimeZoneIntervalContext::TIME() {
  return getToken(PrestoSqlParser::TIME, 0);
}

tree::TerminalNode* PrestoSqlParser::TimeZoneIntervalContext::ZONE() {
  return getToken(PrestoSqlParser::ZONE, 0);
}

PrestoSqlParser::IntervalContext*
PrestoSqlParser::TimeZoneIntervalContext::interval() {
  return getRuleContext<PrestoSqlParser::IntervalContext>(0);
}

PrestoSqlParser::TimeZoneIntervalContext::TimeZoneIntervalContext(
    TimeZoneSpecifierContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::TimeZoneIntervalContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTimeZoneInterval(this);
}
void PrestoSqlParser::TimeZoneIntervalContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTimeZoneInterval(this);
}

antlrcpp::Any PrestoSqlParser::TimeZoneIntervalContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTimeZoneInterval(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TimeZoneStringContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::TimeZoneStringContext::TIME() {
  return getToken(PrestoSqlParser::TIME, 0);
}

tree::TerminalNode* PrestoSqlParser::TimeZoneStringContext::ZONE() {
  return getToken(PrestoSqlParser::ZONE, 0);
}

PrestoSqlParser::StringContext*
PrestoSqlParser::TimeZoneStringContext::string() {
  return getRuleContext<PrestoSqlParser::StringContext>(0);
}

PrestoSqlParser::TimeZoneStringContext::TimeZoneStringContext(
    TimeZoneSpecifierContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::TimeZoneStringContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTimeZoneString(this);
}
void PrestoSqlParser::TimeZoneStringContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTimeZoneString(this);
}

antlrcpp::Any PrestoSqlParser::TimeZoneStringContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTimeZoneString(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::TimeZoneSpecifierContext*
PrestoSqlParser::timeZoneSpecifier() {
  TimeZoneSpecifierContext* _localctx =
      _tracker.createInstance<TimeZoneSpecifierContext>(_ctx, getState());
  enterRule(_localctx, 98, PrestoSqlParser::RuleTimeZoneSpecifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1751);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 224, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TimeZoneIntervalContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1745);
        match(PrestoSqlParser::TIME);
        setState(1746);
        match(PrestoSqlParser::ZONE);
        setState(1747);
        interval();
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TimeZoneStringContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1748);
        match(PrestoSqlParser::TIME);
        setState(1749);
        match(PrestoSqlParser::ZONE);
        setState(1750);
        string();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ComparisonOperatorContext
//------------------------------------------------------------------

PrestoSqlParser::ComparisonOperatorContext::ComparisonOperatorContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::ComparisonOperatorContext::EQ() {
  return getToken(PrestoSqlParser::EQ, 0);
}

tree::TerminalNode* PrestoSqlParser::ComparisonOperatorContext::NEQ() {
  return getToken(PrestoSqlParser::NEQ, 0);
}

tree::TerminalNode* PrestoSqlParser::ComparisonOperatorContext::LT() {
  return getToken(PrestoSqlParser::LT, 0);
}

tree::TerminalNode* PrestoSqlParser::ComparisonOperatorContext::LTE() {
  return getToken(PrestoSqlParser::LTE, 0);
}

tree::TerminalNode* PrestoSqlParser::ComparisonOperatorContext::GT() {
  return getToken(PrestoSqlParser::GT, 0);
}

tree::TerminalNode* PrestoSqlParser::ComparisonOperatorContext::GTE() {
  return getToken(PrestoSqlParser::GTE, 0);
}

size_t PrestoSqlParser::ComparisonOperatorContext::getRuleIndex() const {
  return PrestoSqlParser::RuleComparisonOperator;
}

void PrestoSqlParser::ComparisonOperatorContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterComparisonOperator(this);
}

void PrestoSqlParser::ComparisonOperatorContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitComparisonOperator(this);
}

antlrcpp::Any PrestoSqlParser::ComparisonOperatorContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitComparisonOperator(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ComparisonOperatorContext*
PrestoSqlParser::comparisonOperator() {
  ComparisonOperatorContext* _localctx =
      _tracker.createInstance<ComparisonOperatorContext>(_ctx, getState());
  enterRule(_localctx, 100, PrestoSqlParser::RuleComparisonOperator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1753);
    _la = _input->LA(1);
    if (!(((((_la - 230) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 230)) &
            ((1ULL << (PrestoSqlParser::EQ - 230)) |
             (1ULL << (PrestoSqlParser::NEQ - 230)) |
             (1ULL << (PrestoSqlParser::LT - 230)) |
             (1ULL << (PrestoSqlParser::LTE - 230)) |
             (1ULL << (PrestoSqlParser::GT - 230)) |
             (1ULL << (PrestoSqlParser::GTE - 230)))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ComparisonQuantifierContext
//------------------------------------------------------------------

PrestoSqlParser::ComparisonQuantifierContext::ComparisonQuantifierContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::ComparisonQuantifierContext::ALL() {
  return getToken(PrestoSqlParser::ALL, 0);
}

tree::TerminalNode* PrestoSqlParser::ComparisonQuantifierContext::SOME() {
  return getToken(PrestoSqlParser::SOME, 0);
}

tree::TerminalNode* PrestoSqlParser::ComparisonQuantifierContext::ANY() {
  return getToken(PrestoSqlParser::ANY, 0);
}

size_t PrestoSqlParser::ComparisonQuantifierContext::getRuleIndex() const {
  return PrestoSqlParser::RuleComparisonQuantifier;
}

void PrestoSqlParser::ComparisonQuantifierContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterComparisonQuantifier(this);
}

void PrestoSqlParser::ComparisonQuantifierContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitComparisonQuantifier(this);
}

antlrcpp::Any PrestoSqlParser::ComparisonQuantifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitComparisonQuantifier(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ComparisonQuantifierContext*
PrestoSqlParser::comparisonQuantifier() {
  ComparisonQuantifierContext* _localctx =
      _tracker.createInstance<ComparisonQuantifierContext>(_ctx, getState());
  enterRule(_localctx, 102, PrestoSqlParser::RuleComparisonQuantifier);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1755);
    _la = _input->LA(1);
    if (!(_la == PrestoSqlParser::ALL

          || _la == PrestoSqlParser::ANY || _la == PrestoSqlParser::SOME)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BooleanValueContext
//------------------------------------------------------------------

PrestoSqlParser::BooleanValueContext::BooleanValueContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::BooleanValueContext::TRUE() {
  return getToken(PrestoSqlParser::TRUE, 0);
}

tree::TerminalNode* PrestoSqlParser::BooleanValueContext::FALSE() {
  return getToken(PrestoSqlParser::FALSE, 0);
}

size_t PrestoSqlParser::BooleanValueContext::getRuleIndex() const {
  return PrestoSqlParser::RuleBooleanValue;
}

void PrestoSqlParser::BooleanValueContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBooleanValue(this);
}

void PrestoSqlParser::BooleanValueContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitBooleanValue(this);
}

antlrcpp::Any PrestoSqlParser::BooleanValueContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBooleanValue(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::BooleanValueContext* PrestoSqlParser::booleanValue() {
  BooleanValueContext* _localctx =
      _tracker.createInstance<BooleanValueContext>(_ctx, getState());
  enterRule(_localctx, 104, PrestoSqlParser::RuleBooleanValue);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1757);
    _la = _input->LA(1);
    if (!(_la == PrestoSqlParser::FALSE || _la == PrestoSqlParser::TRUE)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IntervalContext
//------------------------------------------------------------------

PrestoSqlParser::IntervalContext::IntervalContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::IntervalContext::INTERVAL() {
  return getToken(PrestoSqlParser::INTERVAL, 0);
}

PrestoSqlParser::StringContext* PrestoSqlParser::IntervalContext::string() {
  return getRuleContext<PrestoSqlParser::StringContext>(0);
}

std::vector<PrestoSqlParser::IntervalFieldContext*>
PrestoSqlParser::IntervalContext::intervalField() {
  return getRuleContexts<PrestoSqlParser::IntervalFieldContext>();
}

PrestoSqlParser::IntervalFieldContext*
PrestoSqlParser::IntervalContext::intervalField(size_t i) {
  return getRuleContext<PrestoSqlParser::IntervalFieldContext>(i);
}

tree::TerminalNode* PrestoSqlParser::IntervalContext::TO() {
  return getToken(PrestoSqlParser::TO, 0);
}

tree::TerminalNode* PrestoSqlParser::IntervalContext::PLUS() {
  return getToken(PrestoSqlParser::PLUS, 0);
}

tree::TerminalNode* PrestoSqlParser::IntervalContext::MINUS() {
  return getToken(PrestoSqlParser::MINUS, 0);
}

size_t PrestoSqlParser::IntervalContext::getRuleIndex() const {
  return PrestoSqlParser::RuleInterval;
}

void PrestoSqlParser::IntervalContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterInterval(this);
}

void PrestoSqlParser::IntervalContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitInterval(this);
}

antlrcpp::Any PrestoSqlParser::IntervalContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitInterval(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::IntervalContext* PrestoSqlParser::interval() {
  IntervalContext* _localctx =
      _tracker.createInstance<IntervalContext>(_ctx, getState());
  enterRule(_localctx, 106, PrestoSqlParser::RuleInterval);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1759);
    match(PrestoSqlParser::INTERVAL);
    setState(1761);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::PLUS

        || _la == PrestoSqlParser::MINUS) {
      setState(1760);
      antlrcpp::downCast<IntervalContext*>(_localctx)->sign = _input->LT(1);
      _la = _input->LA(1);
      if (!(_la == PrestoSqlParser::PLUS

            || _la == PrestoSqlParser::MINUS)) {
        antlrcpp::downCast<IntervalContext*>(_localctx)->sign =
            _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(1763);
    string();
    setState(1764);
    antlrcpp::downCast<IntervalContext*>(_localctx)->from = intervalField();
    setState(1767);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 226, _ctx)) {
      case 1: {
        setState(1765);
        match(PrestoSqlParser::TO);
        setState(1766);
        antlrcpp::downCast<IntervalContext*>(_localctx)->to = intervalField();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IntervalFieldContext
//------------------------------------------------------------------

PrestoSqlParser::IntervalFieldContext::IntervalFieldContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::IntervalFieldContext::YEAR() {
  return getToken(PrestoSqlParser::YEAR, 0);
}

tree::TerminalNode* PrestoSqlParser::IntervalFieldContext::MONTH() {
  return getToken(PrestoSqlParser::MONTH, 0);
}

tree::TerminalNode* PrestoSqlParser::IntervalFieldContext::DAY() {
  return getToken(PrestoSqlParser::DAY, 0);
}

tree::TerminalNode* PrestoSqlParser::IntervalFieldContext::HOUR() {
  return getToken(PrestoSqlParser::HOUR, 0);
}

tree::TerminalNode* PrestoSqlParser::IntervalFieldContext::MINUTE() {
  return getToken(PrestoSqlParser::MINUTE, 0);
}

tree::TerminalNode* PrestoSqlParser::IntervalFieldContext::SECOND() {
  return getToken(PrestoSqlParser::SECOND, 0);
}

size_t PrestoSqlParser::IntervalFieldContext::getRuleIndex() const {
  return PrestoSqlParser::RuleIntervalField;
}

void PrestoSqlParser::IntervalFieldContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterIntervalField(this);
}

void PrestoSqlParser::IntervalFieldContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitIntervalField(this);
}

antlrcpp::Any PrestoSqlParser::IntervalFieldContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitIntervalField(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::IntervalFieldContext* PrestoSqlParser::intervalField() {
  IntervalFieldContext* _localctx =
      _tracker.createInstance<IntervalFieldContext>(_ctx, getState());
  enterRule(_localctx, 108, PrestoSqlParser::RuleIntervalField);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1769);
    _la = _input->LA(1);
    if (!(_la == PrestoSqlParser::DAY ||
          ((((_la - 91) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 91)) &
            ((1ULL << (PrestoSqlParser::HOUR - 91)) |
             (1ULL << (PrestoSqlParser::MINUTE - 91)) |
             (1ULL << (PrestoSqlParser::MONTH - 91)))) != 0) ||
          _la == PrestoSqlParser::SECOND

          || _la == PrestoSqlParser::YEAR)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NormalFormContext
//------------------------------------------------------------------

PrestoSqlParser::NormalFormContext::NormalFormContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::NormalFormContext::NFD() {
  return getToken(PrestoSqlParser::NFD, 0);
}

tree::TerminalNode* PrestoSqlParser::NormalFormContext::NFC() {
  return getToken(PrestoSqlParser::NFC, 0);
}

tree::TerminalNode* PrestoSqlParser::NormalFormContext::NFKD() {
  return getToken(PrestoSqlParser::NFKD, 0);
}

tree::TerminalNode* PrestoSqlParser::NormalFormContext::NFKC() {
  return getToken(PrestoSqlParser::NFKC, 0);
}

size_t PrestoSqlParser::NormalFormContext::getRuleIndex() const {
  return PrestoSqlParser::RuleNormalForm;
}

void PrestoSqlParser::NormalFormContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNormalForm(this);
}

void PrestoSqlParser::NormalFormContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNormalForm(this);
}

antlrcpp::Any PrestoSqlParser::NormalFormContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNormalForm(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::NormalFormContext* PrestoSqlParser::normalForm() {
  NormalFormContext* _localctx =
      _tracker.createInstance<NormalFormContext>(_ctx, getState());
  enterRule(_localctx, 110, PrestoSqlParser::RuleNormalForm);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1771);
    _la = _input->LA(1);
    if (!(((((_la - 125) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 125)) &
            ((1ULL << (PrestoSqlParser::NFC - 125)) |
             (1ULL << (PrestoSqlParser::NFD - 125)) |
             (1ULL << (PrestoSqlParser::NFKC - 125)) |
             (1ULL << (PrestoSqlParser::NFKD - 125)))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TypesContext
//------------------------------------------------------------------

PrestoSqlParser::TypesContext::TypesContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::TypeContext*>
PrestoSqlParser::TypesContext::type() {
  return getRuleContexts<PrestoSqlParser::TypeContext>();
}

PrestoSqlParser::TypeContext* PrestoSqlParser::TypesContext::type(size_t i) {
  return getRuleContext<PrestoSqlParser::TypeContext>(i);
}

size_t PrestoSqlParser::TypesContext::getRuleIndex() const {
  return PrestoSqlParser::RuleTypes;
}

void PrestoSqlParser::TypesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTypes(this);
}

void PrestoSqlParser::TypesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTypes(this);
}

antlrcpp::Any PrestoSqlParser::TypesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTypes(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::TypesContext* PrestoSqlParser::types() {
  TypesContext* _localctx =
      _tracker.createInstance<TypesContext>(_ctx, getState());
  enterRule(_localctx, 112, PrestoSqlParser::RuleTypes);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1773);
    match(PrestoSqlParser::T__1);
    setState(1782);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) &
          ((1ULL << PrestoSqlParser::ADD) | (1ULL << PrestoSqlParser::ADMIN) |
           (1ULL << PrestoSqlParser::ALL) | (1ULL << PrestoSqlParser::ANALYZE) |
           (1ULL << PrestoSqlParser::ANY) | (1ULL << PrestoSqlParser::ARRAY) |
           (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
           (1ULL << PrestoSqlParser::BEFORE) |
           (1ULL << PrestoSqlParser::BERNOULLI) |
           (1ULL << PrestoSqlParser::CALL) | (1ULL << PrestoSqlParser::CALLED) |
           (1ULL << PrestoSqlParser::CASCADE) |
           (1ULL << PrestoSqlParser::CATALOGS) |
           (1ULL << PrestoSqlParser::COLUMN) |
           (1ULL << PrestoSqlParser::COLUMNS) |
           (1ULL << PrestoSqlParser::COMMENT) |
           (1ULL << PrestoSqlParser::COMMIT) |
           (1ULL << PrestoSqlParser::COMMITTED) |
           (1ULL << PrestoSqlParser::CURRENT) |
           (1ULL << PrestoSqlParser::CURRENT_ROLE) |
           (1ULL << PrestoSqlParser::DATA) | (1ULL << PrestoSqlParser::DATE) |
           (1ULL << PrestoSqlParser::DAY) | (1ULL << PrestoSqlParser::DEFINER) |
           (1ULL << PrestoSqlParser::DESC) |
           (1ULL << PrestoSqlParser::DETERMINISTIC) |
           (1ULL << PrestoSqlParser::DISABLED) |
           (1ULL << PrestoSqlParser::DISTRIBUTED) |
           (1ULL << PrestoSqlParser::ENABLED) |
           (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
        ((((_la - 66) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 66)) &
          ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
           (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
           (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
           (1ULL << (PrestoSqlParser::FETCH - 66)) |
           (1ULL << (PrestoSqlParser::FILTER - 66)) |
           (1ULL << (PrestoSqlParser::FIRST - 66)) |
           (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
           (1ULL << (PrestoSqlParser::FORMAT - 66)) |
           (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
           (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
           (1ULL << (PrestoSqlParser::GRANT - 66)) |
           (1ULL << (PrestoSqlParser::GRANTED - 66)) |
           (1ULL << (PrestoSqlParser::GRANTS - 66)) |
           (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
           (1ULL << (PrestoSqlParser::GROUPS - 66)) |
           (1ULL << (PrestoSqlParser::HOUR - 66)) |
           (1ULL << (PrestoSqlParser::IF - 66)) |
           (1ULL << (PrestoSqlParser::IGNORE - 66)) |
           (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
           (1ULL << (PrestoSqlParser::INPUT - 66)) |
           (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
           (1ULL << (PrestoSqlParser::INVOKER - 66)) |
           (1ULL << (PrestoSqlParser::IO - 66)) |
           (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
           (1ULL << (PrestoSqlParser::JSON - 66)) |
           (1ULL << (PrestoSqlParser::KEY - 66)) |
           (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
           (1ULL << (PrestoSqlParser::LAST - 66)) |
           (1ULL << (PrestoSqlParser::LATERAL - 66)) |
           (1ULL << (PrestoSqlParser::LEVEL - 66)) |
           (1ULL << (PrestoSqlParser::LIMIT - 66)) |
           (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
           (1ULL << (PrestoSqlParser::MAP - 66)) |
           (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
           (1ULL << (PrestoSqlParser::MINUTE - 66)) |
           (1ULL << (PrestoSqlParser::MONTH - 66)) |
           (1ULL << (PrestoSqlParser::NAME - 66)) |
           (1ULL << (PrestoSqlParser::NFC - 66)) |
           (1ULL << (PrestoSqlParser::NFD - 66)) |
           (1ULL << (PrestoSqlParser::NFKC - 66)) |
           (1ULL << (PrestoSqlParser::NFKD - 66)) |
           (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
        ((((_la - 130) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 130)) &
          ((1ULL << (PrestoSqlParser::NONE - 130)) |
           (1ULL << (PrestoSqlParser::NULLIF - 130)) |
           (1ULL << (PrestoSqlParser::NULLS - 130)) |
           (1ULL << (PrestoSqlParser::OF - 130)) |
           (1ULL << (PrestoSqlParser::OFFSET - 130)) |
           (1ULL << (PrestoSqlParser::ONLY - 130)) |
           (1ULL << (PrestoSqlParser::OPTION - 130)) |
           (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
           (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
           (1ULL << (PrestoSqlParser::OVER - 130)) |
           (1ULL << (PrestoSqlParser::PARTITION - 130)) |
           (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
           (1ULL << (PrestoSqlParser::POSITION - 130)) |
           (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
           (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
           (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
           (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
           (1ULL << (PrestoSqlParser::RANGE - 130)) |
           (1ULL << (PrestoSqlParser::READ - 130)) |
           (1ULL << (PrestoSqlParser::REFRESH - 130)) |
           (1ULL << (PrestoSqlParser::RELY - 130)) |
           (1ULL << (PrestoSqlParser::RENAME - 130)) |
           (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
           (1ULL << (PrestoSqlParser::REPLACE - 130)) |
           (1ULL << (PrestoSqlParser::RESET - 130)) |
           (1ULL << (PrestoSqlParser::RESPECT - 130)) |
           (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
           (1ULL << (PrestoSqlParser::RETURN - 130)) |
           (1ULL << (PrestoSqlParser::RETURNS - 130)) |
           (1ULL << (PrestoSqlParser::REVOKE - 130)) |
           (1ULL << (PrestoSqlParser::ROLE - 130)) |
           (1ULL << (PrestoSqlParser::ROLES - 130)) |
           (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
           (1ULL << (PrestoSqlParser::ROW - 130)) |
           (1ULL << (PrestoSqlParser::ROWS - 130)) |
           (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
           (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
           (1ULL << (PrestoSqlParser::SECOND - 130)) |
           (1ULL << (PrestoSqlParser::SECURITY - 130)) |
           (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
           (1ULL << (PrestoSqlParser::SESSION - 130)) |
           (1ULL << (PrestoSqlParser::SET - 130)) |
           (1ULL << (PrestoSqlParser::SETS - 130)) |
           (1ULL << (PrestoSqlParser::SHOW - 130)) |
           (1ULL << (PrestoSqlParser::SOME - 130)) |
           (1ULL << (PrestoSqlParser::SQL - 130)) |
           (1ULL << (PrestoSqlParser::START - 130)) |
           (1ULL << (PrestoSqlParser::STATS - 130)) |
           (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
           (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
           (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
           (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
        ((((_la - 195) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 195)) &
          ((1ULL << (PrestoSqlParser::TABLES - 195)) |
           (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
           (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
           (1ULL << (PrestoSqlParser::TEXT - 195)) |
           (1ULL << (PrestoSqlParser::TIME - 195)) |
           (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
           (1ULL << (PrestoSqlParser::TO - 195)) |
           (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
           (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
           (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
           (1ULL << (PrestoSqlParser::TYPE - 195)) |
           (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
           (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
           (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
           (1ULL << (PrestoSqlParser::UPDATE - 195)) |
           (1ULL << (PrestoSqlParser::USE - 195)) |
           (1ULL << (PrestoSqlParser::USER - 195)) |
           (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
           (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
           (1ULL << (PrestoSqlParser::VERSION - 195)) |
           (1ULL << (PrestoSqlParser::VIEW - 195)) |
           (1ULL << (PrestoSqlParser::WORK - 195)) |
           (1ULL << (PrestoSqlParser::WRITE - 195)) |
           (1ULL << (PrestoSqlParser::YEAR - 195)) |
           (1ULL << (PrestoSqlParser::ZONE - 195)) |
           (1ULL << (PrestoSqlParser::IDENTIFIER - 195)) |
           (1ULL << (PrestoSqlParser::DIGIT_IDENTIFIER - 195)) |
           (1ULL << (PrestoSqlParser::QUOTED_IDENTIFIER - 195)) |
           (1ULL << (PrestoSqlParser::BACKQUOTED_IDENTIFIER - 195)) |
           (1ULL << (PrestoSqlParser::TIME_WITH_TIME_ZONE - 195)) |
           (1ULL << (PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE - 195)) |
           (1ULL << (PrestoSqlParser::DOUBLE_PRECISION - 195)))) != 0)) {
      setState(1774);
      type(0);
      setState(1779);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PrestoSqlParser::T__3) {
        setState(1775);
        match(PrestoSqlParser::T__3);
        setState(1776);
        type(0);
        setState(1781);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1784);
    match(PrestoSqlParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TypeContext
//------------------------------------------------------------------

PrestoSqlParser::TypeContext::TypeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::TypeContext::ARRAY() {
  return getToken(PrestoSqlParser::ARRAY, 0);
}

tree::TerminalNode* PrestoSqlParser::TypeContext::LT() {
  return getToken(PrestoSqlParser::LT, 0);
}

std::vector<PrestoSqlParser::TypeContext*>
PrestoSqlParser::TypeContext::type() {
  return getRuleContexts<PrestoSqlParser::TypeContext>();
}

PrestoSqlParser::TypeContext* PrestoSqlParser::TypeContext::type(size_t i) {
  return getRuleContext<PrestoSqlParser::TypeContext>(i);
}

tree::TerminalNode* PrestoSqlParser::TypeContext::GT() {
  return getToken(PrestoSqlParser::GT, 0);
}

tree::TerminalNode* PrestoSqlParser::TypeContext::MAP() {
  return getToken(PrestoSqlParser::MAP, 0);
}

tree::TerminalNode* PrestoSqlParser::TypeContext::ROW() {
  return getToken(PrestoSqlParser::ROW, 0);
}

std::vector<PrestoSqlParser::IdentifierContext*>
PrestoSqlParser::TypeContext::identifier() {
  return getRuleContexts<PrestoSqlParser::IdentifierContext>();
}

PrestoSqlParser::IdentifierContext* PrestoSqlParser::TypeContext::identifier(
    size_t i) {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(i);
}

PrestoSqlParser::BaseTypeContext* PrestoSqlParser::TypeContext::baseType() {
  return getRuleContext<PrestoSqlParser::BaseTypeContext>(0);
}

std::vector<PrestoSqlParser::TypeParameterContext*>
PrestoSqlParser::TypeContext::typeParameter() {
  return getRuleContexts<PrestoSqlParser::TypeParameterContext>();
}

PrestoSqlParser::TypeParameterContext*
PrestoSqlParser::TypeContext::typeParameter(size_t i) {
  return getRuleContext<PrestoSqlParser::TypeParameterContext>(i);
}

tree::TerminalNode* PrestoSqlParser::TypeContext::INTERVAL() {
  return getToken(PrestoSqlParser::INTERVAL, 0);
}

tree::TerminalNode* PrestoSqlParser::TypeContext::TO() {
  return getToken(PrestoSqlParser::TO, 0);
}

std::vector<PrestoSqlParser::IntervalFieldContext*>
PrestoSqlParser::TypeContext::intervalField() {
  return getRuleContexts<PrestoSqlParser::IntervalFieldContext>();
}

PrestoSqlParser::IntervalFieldContext*
PrestoSqlParser::TypeContext::intervalField(size_t i) {
  return getRuleContext<PrestoSqlParser::IntervalFieldContext>(i);
}

size_t PrestoSqlParser::TypeContext::getRuleIndex() const {
  return PrestoSqlParser::RuleType;
}

void PrestoSqlParser::TypeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterType(this);
}

void PrestoSqlParser::TypeContext::exitRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitType(this);
}

antlrcpp::Any PrestoSqlParser::TypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitType(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::TypeContext* PrestoSqlParser::type() {
  return type(0);
}

PrestoSqlParser::TypeContext* PrestoSqlParser::type(int precedence) {
  ParserRuleContext* parentContext = _ctx;
  size_t parentState = getState();
  PrestoSqlParser::TypeContext* _localctx =
      _tracker.createInstance<TypeContext>(_ctx, parentState);
  PrestoSqlParser::TypeContext* previousContext = _localctx;
  (void)previousContext; // Silence compiler, in case the context is not used by
                         // generated code.
  size_t startState = 114;
  enterRecursionRule(_localctx, 114, PrestoSqlParser::RuleType, precedence);

  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    unrollRecursionContexts(parentContext);
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1833);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 232, _ctx)) {
      case 1: {
        setState(1787);
        match(PrestoSqlParser::ARRAY);
        setState(1788);
        match(PrestoSqlParser::LT);
        setState(1789);
        type(0);
        setState(1790);
        match(PrestoSqlParser::GT);
        break;
      }

      case 2: {
        setState(1792);
        match(PrestoSqlParser::MAP);
        setState(1793);
        match(PrestoSqlParser::LT);
        setState(1794);
        type(0);
        setState(1795);
        match(PrestoSqlParser::T__3);
        setState(1796);
        type(0);
        setState(1797);
        match(PrestoSqlParser::GT);
        break;
      }

      case 3: {
        setState(1799);
        match(PrestoSqlParser::ROW);
        setState(1800);
        match(PrestoSqlParser::T__1);
        setState(1801);
        identifier();
        setState(1802);
        type(0);
        setState(1809);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1803);
          match(PrestoSqlParser::T__3);
          setState(1804);
          identifier();
          setState(1805);
          type(0);
          setState(1811);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1812);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 4: {
        setState(1814);
        baseType();
        setState(1826);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 231, _ctx)) {
          case 1: {
            setState(1815);
            match(PrestoSqlParser::T__1);
            setState(1816);
            typeParameter();
            setState(1821);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == PrestoSqlParser::T__3) {
              setState(1817);
              match(PrestoSqlParser::T__3);
              setState(1818);
              typeParameter();
              setState(1823);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
            setState(1824);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 5: {
        setState(1828);
        match(PrestoSqlParser::INTERVAL);
        setState(1829);
        antlrcpp::downCast<TypeContext*>(_localctx)->from = intervalField();
        setState(1830);
        match(PrestoSqlParser::TO);
        setState(1831);
        antlrcpp::downCast<TypeContext*>(_localctx)->to = intervalField();
        break;
      }

      default:
        break;
    }
    _ctx->stop = _input->LT(-1);
    setState(1839);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 233, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        _localctx =
            _tracker.createInstance<TypeContext>(parentContext, parentState);
        pushNewRecursionContext(_localctx, startState, RuleType);
        setState(1835);

        if (!(precpred(_ctx, 6)))
          throw FailedPredicateException(this, "precpred(_ctx, 6)");
        setState(1836);
        match(PrestoSqlParser::ARRAY);
      }
      setState(1841);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 233, _ctx);
    }
  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }
  return _localctx;
}

//----------------- TypeParameterContext
//------------------------------------------------------------------

PrestoSqlParser::TypeParameterContext::TypeParameterContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::TypeParameterContext::INTEGER_VALUE() {
  return getToken(PrestoSqlParser::INTEGER_VALUE, 0);
}

PrestoSqlParser::TypeContext* PrestoSqlParser::TypeParameterContext::type() {
  return getRuleContext<PrestoSqlParser::TypeContext>(0);
}

size_t PrestoSqlParser::TypeParameterContext::getRuleIndex() const {
  return PrestoSqlParser::RuleTypeParameter;
}

void PrestoSqlParser::TypeParameterContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTypeParameter(this);
}

void PrestoSqlParser::TypeParameterContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTypeParameter(this);
}

antlrcpp::Any PrestoSqlParser::TypeParameterContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTypeParameter(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::TypeParameterContext* PrestoSqlParser::typeParameter() {
  TypeParameterContext* _localctx =
      _tracker.createInstance<TypeParameterContext>(_ctx, getState());
  enterRule(_localctx, 116, PrestoSqlParser::RuleTypeParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1844);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::INTEGER_VALUE: {
        enterOuterAlt(_localctx, 1);
        setState(1842);
        match(PrestoSqlParser::INTEGER_VALUE);
        break;
      }

      case PrestoSqlParser::ADD:
      case PrestoSqlParser::ADMIN:
      case PrestoSqlParser::ALL:
      case PrestoSqlParser::ANALYZE:
      case PrestoSqlParser::ANY:
      case PrestoSqlParser::ARRAY:
      case PrestoSqlParser::ASC:
      case PrestoSqlParser::AT:
      case PrestoSqlParser::BEFORE:
      case PrestoSqlParser::BERNOULLI:
      case PrestoSqlParser::CALL:
      case PrestoSqlParser::CALLED:
      case PrestoSqlParser::CASCADE:
      case PrestoSqlParser::CATALOGS:
      case PrestoSqlParser::COLUMN:
      case PrestoSqlParser::COLUMNS:
      case PrestoSqlParser::COMMENT:
      case PrestoSqlParser::COMMIT:
      case PrestoSqlParser::COMMITTED:
      case PrestoSqlParser::CURRENT:
      case PrestoSqlParser::CURRENT_ROLE:
      case PrestoSqlParser::DATA:
      case PrestoSqlParser::DATE:
      case PrestoSqlParser::DAY:
      case PrestoSqlParser::DEFINER:
      case PrestoSqlParser::DESC:
      case PrestoSqlParser::DETERMINISTIC:
      case PrestoSqlParser::DISABLED:
      case PrestoSqlParser::DISTRIBUTED:
      case PrestoSqlParser::ENABLED:
      case PrestoSqlParser::ENFORCED:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXPLAIN:
      case PrestoSqlParser::EXTERNAL:
      case PrestoSqlParser::FETCH:
      case PrestoSqlParser::FILTER:
      case PrestoSqlParser::FIRST:
      case PrestoSqlParser::FOLLOWING:
      case PrestoSqlParser::FORMAT:
      case PrestoSqlParser::FUNCTION:
      case PrestoSqlParser::FUNCTIONS:
      case PrestoSqlParser::GRANT:
      case PrestoSqlParser::GRANTED:
      case PrestoSqlParser::GRANTS:
      case PrestoSqlParser::GRAPHVIZ:
      case PrestoSqlParser::GROUPS:
      case PrestoSqlParser::HOUR:
      case PrestoSqlParser::IF:
      case PrestoSqlParser::IGNORE:
      case PrestoSqlParser::INCLUDING:
      case PrestoSqlParser::INPUT:
      case PrestoSqlParser::INTERVAL:
      case PrestoSqlParser::INVOKER:
      case PrestoSqlParser::IO:
      case PrestoSqlParser::ISOLATION:
      case PrestoSqlParser::JSON:
      case PrestoSqlParser::KEY:
      case PrestoSqlParser::LANGUAGE:
      case PrestoSqlParser::LAST:
      case PrestoSqlParser::LATERAL:
      case PrestoSqlParser::LEVEL:
      case PrestoSqlParser::LIMIT:
      case PrestoSqlParser::LOGICAL:
      case PrestoSqlParser::MAP:
      case PrestoSqlParser::MATERIALIZED:
      case PrestoSqlParser::MINUTE:
      case PrestoSqlParser::MONTH:
      case PrestoSqlParser::NAME:
      case PrestoSqlParser::NFC:
      case PrestoSqlParser::NFD:
      case PrestoSqlParser::NFKC:
      case PrestoSqlParser::NFKD:
      case PrestoSqlParser::NO:
      case PrestoSqlParser::NONE:
      case PrestoSqlParser::NULLIF:
      case PrestoSqlParser::NULLS:
      case PrestoSqlParser::OF:
      case PrestoSqlParser::OFFSET:
      case PrestoSqlParser::ONLY:
      case PrestoSqlParser::OPTION:
      case PrestoSqlParser::ORDINALITY:
      case PrestoSqlParser::OUTPUT:
      case PrestoSqlParser::OVER:
      case PrestoSqlParser::PARTITION:
      case PrestoSqlParser::PARTITIONS:
      case PrestoSqlParser::POSITION:
      case PrestoSqlParser::PRECEDING:
      case PrestoSqlParser::PRIMARY:
      case PrestoSqlParser::PRIVILEGES:
      case PrestoSqlParser::PROPERTIES:
      case PrestoSqlParser::RANGE:
      case PrestoSqlParser::READ:
      case PrestoSqlParser::REFRESH:
      case PrestoSqlParser::RELY:
      case PrestoSqlParser::RENAME:
      case PrestoSqlParser::REPEATABLE:
      case PrestoSqlParser::REPLACE:
      case PrestoSqlParser::RESET:
      case PrestoSqlParser::RESPECT:
      case PrestoSqlParser::RESTRICT:
      case PrestoSqlParser::RETURN:
      case PrestoSqlParser::RETURNS:
      case PrestoSqlParser::REVOKE:
      case PrestoSqlParser::ROLE:
      case PrestoSqlParser::ROLES:
      case PrestoSqlParser::ROLLBACK:
      case PrestoSqlParser::ROW:
      case PrestoSqlParser::ROWS:
      case PrestoSqlParser::SCHEMA:
      case PrestoSqlParser::SCHEMAS:
      case PrestoSqlParser::SECOND:
      case PrestoSqlParser::SECURITY:
      case PrestoSqlParser::SERIALIZABLE:
      case PrestoSqlParser::SESSION:
      case PrestoSqlParser::SET:
      case PrestoSqlParser::SETS:
      case PrestoSqlParser::SHOW:
      case PrestoSqlParser::SOME:
      case PrestoSqlParser::SQL:
      case PrestoSqlParser::START:
      case PrestoSqlParser::STATS:
      case PrestoSqlParser::SUBSTRING:
      case PrestoSqlParser::SYSTEM:
      case PrestoSqlParser::SYSTEM_TIME:
      case PrestoSqlParser::SYSTEM_VERSION:
      case PrestoSqlParser::TABLES:
      case PrestoSqlParser::TABLESAMPLE:
      case PrestoSqlParser::TEMPORARY:
      case PrestoSqlParser::TEXT:
      case PrestoSqlParser::TIME:
      case PrestoSqlParser::TIMESTAMP:
      case PrestoSqlParser::TO:
      case PrestoSqlParser::TRANSACTION:
      case PrestoSqlParser::TRUNCATE:
      case PrestoSqlParser::TRY_CAST:
      case PrestoSqlParser::TYPE:
      case PrestoSqlParser::UNBOUNDED:
      case PrestoSqlParser::UNCOMMITTED:
      case PrestoSqlParser::UNIQUE:
      case PrestoSqlParser::UPDATE:
      case PrestoSqlParser::USE:
      case PrestoSqlParser::USER:
      case PrestoSqlParser::VALIDATE:
      case PrestoSqlParser::VERBOSE:
      case PrestoSqlParser::VERSION:
      case PrestoSqlParser::VIEW:
      case PrestoSqlParser::WORK:
      case PrestoSqlParser::WRITE:
      case PrestoSqlParser::YEAR:
      case PrestoSqlParser::ZONE:
      case PrestoSqlParser::IDENTIFIER:
      case PrestoSqlParser::DIGIT_IDENTIFIER:
      case PrestoSqlParser::QUOTED_IDENTIFIER:
      case PrestoSqlParser::BACKQUOTED_IDENTIFIER:
      case PrestoSqlParser::TIME_WITH_TIME_ZONE:
      case PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE:
      case PrestoSqlParser::DOUBLE_PRECISION: {
        enterOuterAlt(_localctx, 2);
        setState(1843);
        type(0);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BaseTypeContext
//------------------------------------------------------------------

PrestoSqlParser::BaseTypeContext::BaseTypeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::BaseTypeContext::TIME_WITH_TIME_ZONE() {
  return getToken(PrestoSqlParser::TIME_WITH_TIME_ZONE, 0);
}

tree::TerminalNode*
PrestoSqlParser::BaseTypeContext::TIMESTAMP_WITH_TIME_ZONE() {
  return getToken(PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE, 0);
}

tree::TerminalNode* PrestoSqlParser::BaseTypeContext::DOUBLE_PRECISION() {
  return getToken(PrestoSqlParser::DOUBLE_PRECISION, 0);
}

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::BaseTypeContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

size_t PrestoSqlParser::BaseTypeContext::getRuleIndex() const {
  return PrestoSqlParser::RuleBaseType;
}

void PrestoSqlParser::BaseTypeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBaseType(this);
}

void PrestoSqlParser::BaseTypeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitBaseType(this);
}

antlrcpp::Any PrestoSqlParser::BaseTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBaseType(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::BaseTypeContext* PrestoSqlParser::baseType() {
  BaseTypeContext* _localctx =
      _tracker.createInstance<BaseTypeContext>(_ctx, getState());
  enterRule(_localctx, 118, PrestoSqlParser::RuleBaseType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1850);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::TIME_WITH_TIME_ZONE: {
        enterOuterAlt(_localctx, 1);
        setState(1846);
        match(PrestoSqlParser::TIME_WITH_TIME_ZONE);
        break;
      }

      case PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE: {
        enterOuterAlt(_localctx, 2);
        setState(1847);
        match(PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE);
        break;
      }

      case PrestoSqlParser::DOUBLE_PRECISION: {
        enterOuterAlt(_localctx, 3);
        setState(1848);
        match(PrestoSqlParser::DOUBLE_PRECISION);
        break;
      }

      case PrestoSqlParser::ADD:
      case PrestoSqlParser::ADMIN:
      case PrestoSqlParser::ALL:
      case PrestoSqlParser::ANALYZE:
      case PrestoSqlParser::ANY:
      case PrestoSqlParser::ARRAY:
      case PrestoSqlParser::ASC:
      case PrestoSqlParser::AT:
      case PrestoSqlParser::BEFORE:
      case PrestoSqlParser::BERNOULLI:
      case PrestoSqlParser::CALL:
      case PrestoSqlParser::CALLED:
      case PrestoSqlParser::CASCADE:
      case PrestoSqlParser::CATALOGS:
      case PrestoSqlParser::COLUMN:
      case PrestoSqlParser::COLUMNS:
      case PrestoSqlParser::COMMENT:
      case PrestoSqlParser::COMMIT:
      case PrestoSqlParser::COMMITTED:
      case PrestoSqlParser::CURRENT:
      case PrestoSqlParser::CURRENT_ROLE:
      case PrestoSqlParser::DATA:
      case PrestoSqlParser::DATE:
      case PrestoSqlParser::DAY:
      case PrestoSqlParser::DEFINER:
      case PrestoSqlParser::DESC:
      case PrestoSqlParser::DETERMINISTIC:
      case PrestoSqlParser::DISABLED:
      case PrestoSqlParser::DISTRIBUTED:
      case PrestoSqlParser::ENABLED:
      case PrestoSqlParser::ENFORCED:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXPLAIN:
      case PrestoSqlParser::EXTERNAL:
      case PrestoSqlParser::FETCH:
      case PrestoSqlParser::FILTER:
      case PrestoSqlParser::FIRST:
      case PrestoSqlParser::FOLLOWING:
      case PrestoSqlParser::FORMAT:
      case PrestoSqlParser::FUNCTION:
      case PrestoSqlParser::FUNCTIONS:
      case PrestoSqlParser::GRANT:
      case PrestoSqlParser::GRANTED:
      case PrestoSqlParser::GRANTS:
      case PrestoSqlParser::GRAPHVIZ:
      case PrestoSqlParser::GROUPS:
      case PrestoSqlParser::HOUR:
      case PrestoSqlParser::IF:
      case PrestoSqlParser::IGNORE:
      case PrestoSqlParser::INCLUDING:
      case PrestoSqlParser::INPUT:
      case PrestoSqlParser::INTERVAL:
      case PrestoSqlParser::INVOKER:
      case PrestoSqlParser::IO:
      case PrestoSqlParser::ISOLATION:
      case PrestoSqlParser::JSON:
      case PrestoSqlParser::KEY:
      case PrestoSqlParser::LANGUAGE:
      case PrestoSqlParser::LAST:
      case PrestoSqlParser::LATERAL:
      case PrestoSqlParser::LEVEL:
      case PrestoSqlParser::LIMIT:
      case PrestoSqlParser::LOGICAL:
      case PrestoSqlParser::MAP:
      case PrestoSqlParser::MATERIALIZED:
      case PrestoSqlParser::MINUTE:
      case PrestoSqlParser::MONTH:
      case PrestoSqlParser::NAME:
      case PrestoSqlParser::NFC:
      case PrestoSqlParser::NFD:
      case PrestoSqlParser::NFKC:
      case PrestoSqlParser::NFKD:
      case PrestoSqlParser::NO:
      case PrestoSqlParser::NONE:
      case PrestoSqlParser::NULLIF:
      case PrestoSqlParser::NULLS:
      case PrestoSqlParser::OF:
      case PrestoSqlParser::OFFSET:
      case PrestoSqlParser::ONLY:
      case PrestoSqlParser::OPTION:
      case PrestoSqlParser::ORDINALITY:
      case PrestoSqlParser::OUTPUT:
      case PrestoSqlParser::OVER:
      case PrestoSqlParser::PARTITION:
      case PrestoSqlParser::PARTITIONS:
      case PrestoSqlParser::POSITION:
      case PrestoSqlParser::PRECEDING:
      case PrestoSqlParser::PRIMARY:
      case PrestoSqlParser::PRIVILEGES:
      case PrestoSqlParser::PROPERTIES:
      case PrestoSqlParser::RANGE:
      case PrestoSqlParser::READ:
      case PrestoSqlParser::REFRESH:
      case PrestoSqlParser::RELY:
      case PrestoSqlParser::RENAME:
      case PrestoSqlParser::REPEATABLE:
      case PrestoSqlParser::REPLACE:
      case PrestoSqlParser::RESET:
      case PrestoSqlParser::RESPECT:
      case PrestoSqlParser::RESTRICT:
      case PrestoSqlParser::RETURN:
      case PrestoSqlParser::RETURNS:
      case PrestoSqlParser::REVOKE:
      case PrestoSqlParser::ROLE:
      case PrestoSqlParser::ROLES:
      case PrestoSqlParser::ROLLBACK:
      case PrestoSqlParser::ROW:
      case PrestoSqlParser::ROWS:
      case PrestoSqlParser::SCHEMA:
      case PrestoSqlParser::SCHEMAS:
      case PrestoSqlParser::SECOND:
      case PrestoSqlParser::SECURITY:
      case PrestoSqlParser::SERIALIZABLE:
      case PrestoSqlParser::SESSION:
      case PrestoSqlParser::SET:
      case PrestoSqlParser::SETS:
      case PrestoSqlParser::SHOW:
      case PrestoSqlParser::SOME:
      case PrestoSqlParser::SQL:
      case PrestoSqlParser::START:
      case PrestoSqlParser::STATS:
      case PrestoSqlParser::SUBSTRING:
      case PrestoSqlParser::SYSTEM:
      case PrestoSqlParser::SYSTEM_TIME:
      case PrestoSqlParser::SYSTEM_VERSION:
      case PrestoSqlParser::TABLES:
      case PrestoSqlParser::TABLESAMPLE:
      case PrestoSqlParser::TEMPORARY:
      case PrestoSqlParser::TEXT:
      case PrestoSqlParser::TIME:
      case PrestoSqlParser::TIMESTAMP:
      case PrestoSqlParser::TO:
      case PrestoSqlParser::TRANSACTION:
      case PrestoSqlParser::TRUNCATE:
      case PrestoSqlParser::TRY_CAST:
      case PrestoSqlParser::TYPE:
      case PrestoSqlParser::UNBOUNDED:
      case PrestoSqlParser::UNCOMMITTED:
      case PrestoSqlParser::UNIQUE:
      case PrestoSqlParser::UPDATE:
      case PrestoSqlParser::USE:
      case PrestoSqlParser::USER:
      case PrestoSqlParser::VALIDATE:
      case PrestoSqlParser::VERBOSE:
      case PrestoSqlParser::VERSION:
      case PrestoSqlParser::VIEW:
      case PrestoSqlParser::WORK:
      case PrestoSqlParser::WRITE:
      case PrestoSqlParser::YEAR:
      case PrestoSqlParser::ZONE:
      case PrestoSqlParser::IDENTIFIER:
      case PrestoSqlParser::DIGIT_IDENTIFIER:
      case PrestoSqlParser::QUOTED_IDENTIFIER:
      case PrestoSqlParser::BACKQUOTED_IDENTIFIER: {
        enterOuterAlt(_localctx, 4);
        setState(1849);
        qualifiedName();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WhenClauseContext
//------------------------------------------------------------------

PrestoSqlParser::WhenClauseContext::WhenClauseContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::WhenClauseContext::WHEN() {
  return getToken(PrestoSqlParser::WHEN, 0);
}

tree::TerminalNode* PrestoSqlParser::WhenClauseContext::THEN() {
  return getToken(PrestoSqlParser::THEN, 0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::WhenClauseContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::WhenClauseContext::expression(size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

size_t PrestoSqlParser::WhenClauseContext::getRuleIndex() const {
  return PrestoSqlParser::RuleWhenClause;
}

void PrestoSqlParser::WhenClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterWhenClause(this);
}

void PrestoSqlParser::WhenClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitWhenClause(this);
}

antlrcpp::Any PrestoSqlParser::WhenClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitWhenClause(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::WhenClauseContext* PrestoSqlParser::whenClause() {
  WhenClauseContext* _localctx =
      _tracker.createInstance<WhenClauseContext>(_ctx, getState());
  enterRule(_localctx, 120, PrestoSqlParser::RuleWhenClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1852);
    match(PrestoSqlParser::WHEN);
    setState(1853);
    antlrcpp::downCast<WhenClauseContext*>(_localctx)->condition = expression();
    setState(1854);
    match(PrestoSqlParser::THEN);
    setState(1855);
    antlrcpp::downCast<WhenClauseContext*>(_localctx)->result = expression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FilterContext
//------------------------------------------------------------------

PrestoSqlParser::FilterContext::FilterContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::FilterContext::FILTER() {
  return getToken(PrestoSqlParser::FILTER, 0);
}

tree::TerminalNode* PrestoSqlParser::FilterContext::WHERE() {
  return getToken(PrestoSqlParser::WHERE, 0);
}

PrestoSqlParser::BooleanExpressionContext*
PrestoSqlParser::FilterContext::booleanExpression() {
  return getRuleContext<PrestoSqlParser::BooleanExpressionContext>(0);
}

size_t PrestoSqlParser::FilterContext::getRuleIndex() const {
  return PrestoSqlParser::RuleFilter;
}

void PrestoSqlParser::FilterContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterFilter(this);
}

void PrestoSqlParser::FilterContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitFilter(this);
}

antlrcpp::Any PrestoSqlParser::FilterContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitFilter(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::FilterContext* PrestoSqlParser::filter() {
  FilterContext* _localctx =
      _tracker.createInstance<FilterContext>(_ctx, getState());
  enterRule(_localctx, 122, PrestoSqlParser::RuleFilter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1857);
    match(PrestoSqlParser::FILTER);
    setState(1858);
    match(PrestoSqlParser::T__1);
    setState(1859);
    match(PrestoSqlParser::WHERE);
    setState(1860);
    booleanExpression(0);
    setState(1861);
    match(PrestoSqlParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OverContext
//------------------------------------------------------------------

PrestoSqlParser::OverContext::OverContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::OverContext::OVER() {
  return getToken(PrestoSqlParser::OVER, 0);
}

tree::TerminalNode* PrestoSqlParser::OverContext::PARTITION() {
  return getToken(PrestoSqlParser::PARTITION, 0);
}

std::vector<tree::TerminalNode*> PrestoSqlParser::OverContext::BY() {
  return getTokens(PrestoSqlParser::BY);
}

tree::TerminalNode* PrestoSqlParser::OverContext::BY(size_t i) {
  return getToken(PrestoSqlParser::BY, i);
}

tree::TerminalNode* PrestoSqlParser::OverContext::ORDER() {
  return getToken(PrestoSqlParser::ORDER, 0);
}

std::vector<PrestoSqlParser::SortItemContext*>
PrestoSqlParser::OverContext::sortItem() {
  return getRuleContexts<PrestoSqlParser::SortItemContext>();
}

PrestoSqlParser::SortItemContext* PrestoSqlParser::OverContext::sortItem(
    size_t i) {
  return getRuleContext<PrestoSqlParser::SortItemContext>(i);
}

PrestoSqlParser::WindowFrameContext*
PrestoSqlParser::OverContext::windowFrame() {
  return getRuleContext<PrestoSqlParser::WindowFrameContext>(0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::OverContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext* PrestoSqlParser::OverContext::expression(
    size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

size_t PrestoSqlParser::OverContext::getRuleIndex() const {
  return PrestoSqlParser::RuleOver;
}

void PrestoSqlParser::OverContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterOver(this);
}

void PrestoSqlParser::OverContext::exitRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitOver(this);
}

antlrcpp::Any PrestoSqlParser::OverContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitOver(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::OverContext* PrestoSqlParser::over() {
  OverContext* _localctx =
      _tracker.createInstance<OverContext>(_ctx, getState());
  enterRule(_localctx, 124, PrestoSqlParser::RuleOver);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1863);
    match(PrestoSqlParser::OVER);
    setState(1864);
    match(PrestoSqlParser::T__1);
    setState(1875);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::PARTITION) {
      setState(1865);
      match(PrestoSqlParser::PARTITION);
      setState(1866);
      match(PrestoSqlParser::BY);
      setState(1867);
      antlrcpp::downCast<OverContext*>(_localctx)->expressionContext =
          expression();
      antlrcpp::downCast<OverContext*>(_localctx)->partition.push_back(
          antlrcpp::downCast<OverContext*>(_localctx)->expressionContext);
      setState(1872);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PrestoSqlParser::T__3) {
        setState(1868);
        match(PrestoSqlParser::T__3);
        setState(1869);
        antlrcpp::downCast<OverContext*>(_localctx)->expressionContext =
            expression();
        antlrcpp::downCast<OverContext*>(_localctx)->partition.push_back(
            antlrcpp::downCast<OverContext*>(_localctx)->expressionContext);
        setState(1874);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1887);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::ORDER) {
      setState(1877);
      match(PrestoSqlParser::ORDER);
      setState(1878);
      match(PrestoSqlParser::BY);
      setState(1879);
      sortItem();
      setState(1884);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PrestoSqlParser::T__3) {
        setState(1880);
        match(PrestoSqlParser::T__3);
        setState(1881);
        sortItem();
        setState(1886);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1890);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::GROUPS || _la == PrestoSqlParser::RANGE

        || _la == PrestoSqlParser::ROWS) {
      setState(1889);
      windowFrame();
    }
    setState(1892);
    match(PrestoSqlParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WindowFrameContext
//------------------------------------------------------------------

PrestoSqlParser::WindowFrameContext::WindowFrameContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::WindowFrameContext::RANGE() {
  return getToken(PrestoSqlParser::RANGE, 0);
}

std::vector<PrestoSqlParser::FrameBoundContext*>
PrestoSqlParser::WindowFrameContext::frameBound() {
  return getRuleContexts<PrestoSqlParser::FrameBoundContext>();
}

PrestoSqlParser::FrameBoundContext*
PrestoSqlParser::WindowFrameContext::frameBound(size_t i) {
  return getRuleContext<PrestoSqlParser::FrameBoundContext>(i);
}

tree::TerminalNode* PrestoSqlParser::WindowFrameContext::ROWS() {
  return getToken(PrestoSqlParser::ROWS, 0);
}

tree::TerminalNode* PrestoSqlParser::WindowFrameContext::GROUPS() {
  return getToken(PrestoSqlParser::GROUPS, 0);
}

tree::TerminalNode* PrestoSqlParser::WindowFrameContext::BETWEEN() {
  return getToken(PrestoSqlParser::BETWEEN, 0);
}

tree::TerminalNode* PrestoSqlParser::WindowFrameContext::AND() {
  return getToken(PrestoSqlParser::AND, 0);
}

size_t PrestoSqlParser::WindowFrameContext::getRuleIndex() const {
  return PrestoSqlParser::RuleWindowFrame;
}

void PrestoSqlParser::WindowFrameContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterWindowFrame(this);
}

void PrestoSqlParser::WindowFrameContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitWindowFrame(this);
}

antlrcpp::Any PrestoSqlParser::WindowFrameContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitWindowFrame(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::WindowFrameContext* PrestoSqlParser::windowFrame() {
  WindowFrameContext* _localctx =
      _tracker.createInstance<WindowFrameContext>(_ctx, getState());
  enterRule(_localctx, 126, PrestoSqlParser::RuleWindowFrame);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1918);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 241, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(1894);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::RANGE);
        setState(1895);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(1896);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::ROWS);
        setState(1897);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(1898);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::GROUPS);
        setState(1899);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        break;
      }

      case 4: {
        enterOuterAlt(_localctx, 4);
        setState(1900);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::RANGE);
        setState(1901);
        match(PrestoSqlParser::BETWEEN);
        setState(1902);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        setState(1903);
        match(PrestoSqlParser::AND);
        setState(1904);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->end = frameBound();
        break;
      }

      case 5: {
        enterOuterAlt(_localctx, 5);
        setState(1906);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::ROWS);
        setState(1907);
        match(PrestoSqlParser::BETWEEN);
        setState(1908);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        setState(1909);
        match(PrestoSqlParser::AND);
        setState(1910);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->end = frameBound();
        break;
      }

      case 6: {
        enterOuterAlt(_localctx, 6);
        setState(1912);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::GROUPS);
        setState(1913);
        match(PrestoSqlParser::BETWEEN);
        setState(1914);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        setState(1915);
        match(PrestoSqlParser::AND);
        setState(1916);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->end = frameBound();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FrameBoundContext
//------------------------------------------------------------------

PrestoSqlParser::FrameBoundContext::FrameBoundContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::FrameBoundContext::getRuleIndex() const {
  return PrestoSqlParser::RuleFrameBound;
}

void PrestoSqlParser::FrameBoundContext::copyFrom(FrameBoundContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- BoundedFrameContext
//------------------------------------------------------------------

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::BoundedFrameContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::BoundedFrameContext::PRECEDING() {
  return getToken(PrestoSqlParser::PRECEDING, 0);
}

tree::TerminalNode* PrestoSqlParser::BoundedFrameContext::FOLLOWING() {
  return getToken(PrestoSqlParser::FOLLOWING, 0);
}

PrestoSqlParser::BoundedFrameContext::BoundedFrameContext(
    FrameBoundContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::BoundedFrameContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBoundedFrame(this);
}
void PrestoSqlParser::BoundedFrameContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitBoundedFrame(this);
}

antlrcpp::Any PrestoSqlParser::BoundedFrameContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBoundedFrame(this);
  else
    return visitor->visitChildren(this);
}
//----------------- UnboundedFrameContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::UnboundedFrameContext::UNBOUNDED() {
  return getToken(PrestoSqlParser::UNBOUNDED, 0);
}

tree::TerminalNode* PrestoSqlParser::UnboundedFrameContext::PRECEDING() {
  return getToken(PrestoSqlParser::PRECEDING, 0);
}

tree::TerminalNode* PrestoSqlParser::UnboundedFrameContext::FOLLOWING() {
  return getToken(PrestoSqlParser::FOLLOWING, 0);
}

PrestoSqlParser::UnboundedFrameContext::UnboundedFrameContext(
    FrameBoundContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::UnboundedFrameContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnboundedFrame(this);
}
void PrestoSqlParser::UnboundedFrameContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnboundedFrame(this);
}

antlrcpp::Any PrestoSqlParser::UnboundedFrameContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUnboundedFrame(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CurrentRowBoundContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CurrentRowBoundContext::CURRENT() {
  return getToken(PrestoSqlParser::CURRENT, 0);
}

tree::TerminalNode* PrestoSqlParser::CurrentRowBoundContext::ROW() {
  return getToken(PrestoSqlParser::ROW, 0);
}

PrestoSqlParser::CurrentRowBoundContext::CurrentRowBoundContext(
    FrameBoundContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CurrentRowBoundContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCurrentRowBound(this);
}
void PrestoSqlParser::CurrentRowBoundContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCurrentRowBound(this);
}

antlrcpp::Any PrestoSqlParser::CurrentRowBoundContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCurrentRowBound(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::FrameBoundContext* PrestoSqlParser::frameBound() {
  FrameBoundContext* _localctx =
      _tracker.createInstance<FrameBoundContext>(_ctx, getState());
  enterRule(_localctx, 128, PrestoSqlParser::RuleFrameBound);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1929);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 242, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UnboundedFrameContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1920);
        match(PrestoSqlParser::UNBOUNDED);
        setState(1921);
        antlrcpp::downCast<UnboundedFrameContext*>(_localctx)->boundType =
            match(PrestoSqlParser::PRECEDING);
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UnboundedFrameContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1922);
        match(PrestoSqlParser::UNBOUNDED);
        setState(1923);
        antlrcpp::downCast<UnboundedFrameContext*>(_localctx)->boundType =
            match(PrestoSqlParser::FOLLOWING);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CurrentRowBoundContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(1924);
        match(PrestoSqlParser::CURRENT);
        setState(1925);
        match(PrestoSqlParser::ROW);
        break;
      }

      case 4: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::BoundedFrameContext>(
                _localctx);
        enterOuterAlt(_localctx, 4);
        setState(1926);
        expression();
        setState(1927);
        antlrcpp::downCast<BoundedFrameContext*>(_localctx)->boundType =
            _input->LT(1);
        _la = _input->LA(1);
        if (!(_la == PrestoSqlParser::FOLLOWING ||
              _la == PrestoSqlParser::PRECEDING)) {
          antlrcpp::downCast<BoundedFrameContext*>(_localctx)->boundType =
              _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- UpdateAssignmentContext
//------------------------------------------------------------------

PrestoSqlParser::UpdateAssignmentContext::UpdateAssignmentContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::UpdateAssignmentContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

tree::TerminalNode* PrestoSqlParser::UpdateAssignmentContext::EQ() {
  return getToken(PrestoSqlParser::EQ, 0);
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::UpdateAssignmentContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

size_t PrestoSqlParser::UpdateAssignmentContext::getRuleIndex() const {
  return PrestoSqlParser::RuleUpdateAssignment;
}

void PrestoSqlParser::UpdateAssignmentContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterUpdateAssignment(this);
}

void PrestoSqlParser::UpdateAssignmentContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitUpdateAssignment(this);
}

antlrcpp::Any PrestoSqlParser::UpdateAssignmentContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUpdateAssignment(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::UpdateAssignmentContext* PrestoSqlParser::updateAssignment() {
  UpdateAssignmentContext* _localctx =
      _tracker.createInstance<UpdateAssignmentContext>(_ctx, getState());
  enterRule(_localctx, 130, PrestoSqlParser::RuleUpdateAssignment);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1931);
    identifier();
    setState(1932);
    match(PrestoSqlParser::EQ);
    setState(1933);
    expression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExplainOptionContext
//------------------------------------------------------------------

PrestoSqlParser::ExplainOptionContext::ExplainOptionContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::ExplainOptionContext::getRuleIndex() const {
  return PrestoSqlParser::RuleExplainOption;
}

void PrestoSqlParser::ExplainOptionContext::copyFrom(
    ExplainOptionContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ExplainFormatContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ExplainFormatContext::FORMAT() {
  return getToken(PrestoSqlParser::FORMAT, 0);
}

tree::TerminalNode* PrestoSqlParser::ExplainFormatContext::TEXT() {
  return getToken(PrestoSqlParser::TEXT, 0);
}

tree::TerminalNode* PrestoSqlParser::ExplainFormatContext::GRAPHVIZ() {
  return getToken(PrestoSqlParser::GRAPHVIZ, 0);
}

tree::TerminalNode* PrestoSqlParser::ExplainFormatContext::JSON() {
  return getToken(PrestoSqlParser::JSON, 0);
}

PrestoSqlParser::ExplainFormatContext::ExplainFormatContext(
    ExplainOptionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ExplainFormatContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterExplainFormat(this);
}
void PrestoSqlParser::ExplainFormatContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitExplainFormat(this);
}

antlrcpp::Any PrestoSqlParser::ExplainFormatContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExplainFormat(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ExplainTypeContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ExplainTypeContext::TYPE() {
  return getToken(PrestoSqlParser::TYPE, 0);
}

tree::TerminalNode* PrestoSqlParser::ExplainTypeContext::LOGICAL() {
  return getToken(PrestoSqlParser::LOGICAL, 0);
}

tree::TerminalNode* PrestoSqlParser::ExplainTypeContext::DISTRIBUTED() {
  return getToken(PrestoSqlParser::DISTRIBUTED, 0);
}

tree::TerminalNode* PrestoSqlParser::ExplainTypeContext::VALIDATE() {
  return getToken(PrestoSqlParser::VALIDATE, 0);
}

tree::TerminalNode* PrestoSqlParser::ExplainTypeContext::IO() {
  return getToken(PrestoSqlParser::IO, 0);
}

PrestoSqlParser::ExplainTypeContext::ExplainTypeContext(
    ExplainOptionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ExplainTypeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterExplainType(this);
}
void PrestoSqlParser::ExplainTypeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitExplainType(this);
}

antlrcpp::Any PrestoSqlParser::ExplainTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExplainType(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::ExplainOptionContext* PrestoSqlParser::explainOption() {
  ExplainOptionContext* _localctx =
      _tracker.createInstance<ExplainOptionContext>(_ctx, getState());
  enterRule(_localctx, 132, PrestoSqlParser::RuleExplainOption);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1939);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::FORMAT: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ExplainFormatContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1935);
        match(PrestoSqlParser::FORMAT);
        setState(1936);
        antlrcpp::downCast<ExplainFormatContext*>(_localctx)->value =
            _input->LT(1);
        _la = _input->LA(1);
        if (!(_la == PrestoSqlParser::GRAPHVIZ

              || _la == PrestoSqlParser::JSON ||
              _la == PrestoSqlParser::TEXT)) {
          antlrcpp::downCast<ExplainFormatContext*>(_localctx)->value =
              _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }

      case PrestoSqlParser::TYPE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ExplainTypeContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1937);
        match(PrestoSqlParser::TYPE);
        setState(1938);
        antlrcpp::downCast<ExplainTypeContext*>(_localctx)->value =
            _input->LT(1);
        _la = _input->LA(1);
        if (!(((((_la - 58) & ~0x3fULL) == 0) &&
               ((1ULL << (_la - 58)) &
                ((1ULL << (PrestoSqlParser::DISTRIBUTED - 58)) |
                 (1ULL << (PrestoSqlParser::IO - 58)) |
                 (1ULL << (PrestoSqlParser::LOGICAL - 58)))) != 0) ||
              _la == PrestoSqlParser::VALIDATE)) {
          antlrcpp::downCast<ExplainTypeContext*>(_localctx)->value =
              _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TransactionModeContext
//------------------------------------------------------------------

PrestoSqlParser::TransactionModeContext::TransactionModeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::TransactionModeContext::getRuleIndex() const {
  return PrestoSqlParser::RuleTransactionMode;
}

void PrestoSqlParser::TransactionModeContext::copyFrom(
    TransactionModeContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- TransactionAccessModeContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::TransactionAccessModeContext::READ() {
  return getToken(PrestoSqlParser::READ, 0);
}

tree::TerminalNode* PrestoSqlParser::TransactionAccessModeContext::ONLY() {
  return getToken(PrestoSqlParser::ONLY, 0);
}

tree::TerminalNode* PrestoSqlParser::TransactionAccessModeContext::WRITE() {
  return getToken(PrestoSqlParser::WRITE, 0);
}

PrestoSqlParser::TransactionAccessModeContext::TransactionAccessModeContext(
    TransactionModeContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::TransactionAccessModeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTransactionAccessMode(this);
}
void PrestoSqlParser::TransactionAccessModeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTransactionAccessMode(this);
}

antlrcpp::Any PrestoSqlParser::TransactionAccessModeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTransactionAccessMode(this);
  else
    return visitor->visitChildren(this);
}
//----------------- IsolationLevelContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::IsolationLevelContext::ISOLATION() {
  return getToken(PrestoSqlParser::ISOLATION, 0);
}

tree::TerminalNode* PrestoSqlParser::IsolationLevelContext::LEVEL() {
  return getToken(PrestoSqlParser::LEVEL, 0);
}

PrestoSqlParser::LevelOfIsolationContext*
PrestoSqlParser::IsolationLevelContext::levelOfIsolation() {
  return getRuleContext<PrestoSqlParser::LevelOfIsolationContext>(0);
}

PrestoSqlParser::IsolationLevelContext::IsolationLevelContext(
    TransactionModeContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::IsolationLevelContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterIsolationLevel(this);
}
void PrestoSqlParser::IsolationLevelContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitIsolationLevel(this);
}

antlrcpp::Any PrestoSqlParser::IsolationLevelContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitIsolationLevel(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::TransactionModeContext* PrestoSqlParser::transactionMode() {
  TransactionModeContext* _localctx =
      _tracker.createInstance<TransactionModeContext>(_ctx, getState());
  enterRule(_localctx, 134, PrestoSqlParser::RuleTransactionMode);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1946);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::ISOLATION: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::IsolationLevelContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1941);
        match(PrestoSqlParser::ISOLATION);
        setState(1942);
        match(PrestoSqlParser::LEVEL);
        setState(1943);
        levelOfIsolation();
        break;
      }

      case PrestoSqlParser::READ: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::TransactionAccessModeContext>(
                    _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1944);
        match(PrestoSqlParser::READ);
        setState(1945);
        antlrcpp::downCast<TransactionAccessModeContext*>(_localctx)
            ->accessMode = _input->LT(1);
        _la = _input->LA(1);
        if (!(_la == PrestoSqlParser::ONLY || _la == PrestoSqlParser::WRITE)) {
          antlrcpp::downCast<TransactionAccessModeContext*>(_localctx)
              ->accessMode = _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LevelOfIsolationContext
//------------------------------------------------------------------

PrestoSqlParser::LevelOfIsolationContext::LevelOfIsolationContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::LevelOfIsolationContext::getRuleIndex() const {
  return PrestoSqlParser::RuleLevelOfIsolation;
}

void PrestoSqlParser::LevelOfIsolationContext::copyFrom(
    LevelOfIsolationContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- ReadUncommittedContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ReadUncommittedContext::READ() {
  return getToken(PrestoSqlParser::READ, 0);
}

tree::TerminalNode* PrestoSqlParser::ReadUncommittedContext::UNCOMMITTED() {
  return getToken(PrestoSqlParser::UNCOMMITTED, 0);
}

PrestoSqlParser::ReadUncommittedContext::ReadUncommittedContext(
    LevelOfIsolationContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ReadUncommittedContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterReadUncommitted(this);
}
void PrestoSqlParser::ReadUncommittedContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitReadUncommitted(this);
}

antlrcpp::Any PrestoSqlParser::ReadUncommittedContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitReadUncommitted(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SerializableContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::SerializableContext::SERIALIZABLE() {
  return getToken(PrestoSqlParser::SERIALIZABLE, 0);
}

PrestoSqlParser::SerializableContext::SerializableContext(
    LevelOfIsolationContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SerializableContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSerializable(this);
}
void PrestoSqlParser::SerializableContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSerializable(this);
}

antlrcpp::Any PrestoSqlParser::SerializableContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSerializable(this);
  else
    return visitor->visitChildren(this);
}
//----------------- ReadCommittedContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::ReadCommittedContext::READ() {
  return getToken(PrestoSqlParser::READ, 0);
}

tree::TerminalNode* PrestoSqlParser::ReadCommittedContext::COMMITTED() {
  return getToken(PrestoSqlParser::COMMITTED, 0);
}

PrestoSqlParser::ReadCommittedContext::ReadCommittedContext(
    LevelOfIsolationContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::ReadCommittedContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterReadCommitted(this);
}
void PrestoSqlParser::ReadCommittedContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitReadCommitted(this);
}

antlrcpp::Any PrestoSqlParser::ReadCommittedContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitReadCommitted(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RepeatableReadContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RepeatableReadContext::REPEATABLE() {
  return getToken(PrestoSqlParser::REPEATABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::RepeatableReadContext::READ() {
  return getToken(PrestoSqlParser::READ, 0);
}

PrestoSqlParser::RepeatableReadContext::RepeatableReadContext(
    LevelOfIsolationContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RepeatableReadContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRepeatableRead(this);
}
void PrestoSqlParser::RepeatableReadContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRepeatableRead(this);
}

antlrcpp::Any PrestoSqlParser::RepeatableReadContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRepeatableRead(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::LevelOfIsolationContext* PrestoSqlParser::levelOfIsolation() {
  LevelOfIsolationContext* _localctx =
      _tracker.createInstance<LevelOfIsolationContext>(_ctx, getState());
  enterRule(_localctx, 136, PrestoSqlParser::RuleLevelOfIsolation);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1955);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 245, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ReadUncommittedContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1948);
        match(PrestoSqlParser::READ);
        setState(1949);
        match(PrestoSqlParser::UNCOMMITTED);
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ReadCommittedContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1950);
        match(PrestoSqlParser::READ);
        setState(1951);
        match(PrestoSqlParser::COMMITTED);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RepeatableReadContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(1952);
        match(PrestoSqlParser::REPEATABLE);
        setState(1953);
        match(PrestoSqlParser::READ);
        break;
      }

      case 4: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SerializableContext>(
                _localctx);
        enterOuterAlt(_localctx, 4);
        setState(1954);
        match(PrestoSqlParser::SERIALIZABLE);
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CallArgumentContext
//------------------------------------------------------------------

PrestoSqlParser::CallArgumentContext::CallArgumentContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::CallArgumentContext::getRuleIndex() const {
  return PrestoSqlParser::RuleCallArgument;
}

void PrestoSqlParser::CallArgumentContext::copyFrom(CallArgumentContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- PositionalArgumentContext
//------------------------------------------------------------------

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::PositionalArgumentContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

PrestoSqlParser::PositionalArgumentContext::PositionalArgumentContext(
    CallArgumentContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::PositionalArgumentContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterPositionalArgument(this);
}
void PrestoSqlParser::PositionalArgumentContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitPositionalArgument(this);
}

antlrcpp::Any PrestoSqlParser::PositionalArgumentContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitPositionalArgument(this);
  else
    return visitor->visitChildren(this);
}
//----------------- NamedArgumentContext
//------------------------------------------------------------------

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::NamedArgumentContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::NamedArgumentContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

PrestoSqlParser::NamedArgumentContext::NamedArgumentContext(
    CallArgumentContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::NamedArgumentContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNamedArgument(this);
}
void PrestoSqlParser::NamedArgumentContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNamedArgument(this);
}

antlrcpp::Any PrestoSqlParser::NamedArgumentContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNamedArgument(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::CallArgumentContext* PrestoSqlParser::callArgument() {
  CallArgumentContext* _localctx =
      _tracker.createInstance<CallArgumentContext>(_ctx, getState());
  enterRule(_localctx, 138, PrestoSqlParser::RuleCallArgument);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1962);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 246, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::PositionalArgumentContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1957);
        expression();
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::NamedArgumentContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1958);
        identifier();
        setState(1959);
        match(PrestoSqlParser::T__8);
        setState(1960);
        expression();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrivilegeContext
//------------------------------------------------------------------

PrestoSqlParser::PrivilegeContext::PrivilegeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::PrivilegeContext::SELECT() {
  return getToken(PrestoSqlParser::SELECT, 0);
}

tree::TerminalNode* PrestoSqlParser::PrivilegeContext::DELETE() {
  return getToken(PrestoSqlParser::DELETE, 0);
}

tree::TerminalNode* PrestoSqlParser::PrivilegeContext::INSERT() {
  return getToken(PrestoSqlParser::INSERT, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::PrivilegeContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

size_t PrestoSqlParser::PrivilegeContext::getRuleIndex() const {
  return PrestoSqlParser::RulePrivilege;
}

void PrestoSqlParser::PrivilegeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrivilege(this);
}

void PrestoSqlParser::PrivilegeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrivilege(this);
}

antlrcpp::Any PrestoSqlParser::PrivilegeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitPrivilege(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::PrivilegeContext* PrestoSqlParser::privilege() {
  PrivilegeContext* _localctx =
      _tracker.createInstance<PrivilegeContext>(_ctx, getState());
  enterRule(_localctx, 140, PrestoSqlParser::RulePrivilege);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1968);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::SELECT: {
        enterOuterAlt(_localctx, 1);
        setState(1964);
        match(PrestoSqlParser::SELECT);
        break;
      }

      case PrestoSqlParser::DELETE: {
        enterOuterAlt(_localctx, 2);
        setState(1965);
        match(PrestoSqlParser::DELETE);
        break;
      }

      case PrestoSqlParser::INSERT: {
        enterOuterAlt(_localctx, 3);
        setState(1966);
        match(PrestoSqlParser::INSERT);
        break;
      }

      case PrestoSqlParser::ADD:
      case PrestoSqlParser::ADMIN:
      case PrestoSqlParser::ALL:
      case PrestoSqlParser::ANALYZE:
      case PrestoSqlParser::ANY:
      case PrestoSqlParser::ARRAY:
      case PrestoSqlParser::ASC:
      case PrestoSqlParser::AT:
      case PrestoSqlParser::BEFORE:
      case PrestoSqlParser::BERNOULLI:
      case PrestoSqlParser::CALL:
      case PrestoSqlParser::CALLED:
      case PrestoSqlParser::CASCADE:
      case PrestoSqlParser::CATALOGS:
      case PrestoSqlParser::COLUMN:
      case PrestoSqlParser::COLUMNS:
      case PrestoSqlParser::COMMENT:
      case PrestoSqlParser::COMMIT:
      case PrestoSqlParser::COMMITTED:
      case PrestoSqlParser::CURRENT:
      case PrestoSqlParser::CURRENT_ROLE:
      case PrestoSqlParser::DATA:
      case PrestoSqlParser::DATE:
      case PrestoSqlParser::DAY:
      case PrestoSqlParser::DEFINER:
      case PrestoSqlParser::DESC:
      case PrestoSqlParser::DETERMINISTIC:
      case PrestoSqlParser::DISABLED:
      case PrestoSqlParser::DISTRIBUTED:
      case PrestoSqlParser::ENABLED:
      case PrestoSqlParser::ENFORCED:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXPLAIN:
      case PrestoSqlParser::EXTERNAL:
      case PrestoSqlParser::FETCH:
      case PrestoSqlParser::FILTER:
      case PrestoSqlParser::FIRST:
      case PrestoSqlParser::FOLLOWING:
      case PrestoSqlParser::FORMAT:
      case PrestoSqlParser::FUNCTION:
      case PrestoSqlParser::FUNCTIONS:
      case PrestoSqlParser::GRANT:
      case PrestoSqlParser::GRANTED:
      case PrestoSqlParser::GRANTS:
      case PrestoSqlParser::GRAPHVIZ:
      case PrestoSqlParser::GROUPS:
      case PrestoSqlParser::HOUR:
      case PrestoSqlParser::IF:
      case PrestoSqlParser::IGNORE:
      case PrestoSqlParser::INCLUDING:
      case PrestoSqlParser::INPUT:
      case PrestoSqlParser::INTERVAL:
      case PrestoSqlParser::INVOKER:
      case PrestoSqlParser::IO:
      case PrestoSqlParser::ISOLATION:
      case PrestoSqlParser::JSON:
      case PrestoSqlParser::KEY:
      case PrestoSqlParser::LANGUAGE:
      case PrestoSqlParser::LAST:
      case PrestoSqlParser::LATERAL:
      case PrestoSqlParser::LEVEL:
      case PrestoSqlParser::LIMIT:
      case PrestoSqlParser::LOGICAL:
      case PrestoSqlParser::MAP:
      case PrestoSqlParser::MATERIALIZED:
      case PrestoSqlParser::MINUTE:
      case PrestoSqlParser::MONTH:
      case PrestoSqlParser::NAME:
      case PrestoSqlParser::NFC:
      case PrestoSqlParser::NFD:
      case PrestoSqlParser::NFKC:
      case PrestoSqlParser::NFKD:
      case PrestoSqlParser::NO:
      case PrestoSqlParser::NONE:
      case PrestoSqlParser::NULLIF:
      case PrestoSqlParser::NULLS:
      case PrestoSqlParser::OF:
      case PrestoSqlParser::OFFSET:
      case PrestoSqlParser::ONLY:
      case PrestoSqlParser::OPTION:
      case PrestoSqlParser::ORDINALITY:
      case PrestoSqlParser::OUTPUT:
      case PrestoSqlParser::OVER:
      case PrestoSqlParser::PARTITION:
      case PrestoSqlParser::PARTITIONS:
      case PrestoSqlParser::POSITION:
      case PrestoSqlParser::PRECEDING:
      case PrestoSqlParser::PRIMARY:
      case PrestoSqlParser::PRIVILEGES:
      case PrestoSqlParser::PROPERTIES:
      case PrestoSqlParser::RANGE:
      case PrestoSqlParser::READ:
      case PrestoSqlParser::REFRESH:
      case PrestoSqlParser::RELY:
      case PrestoSqlParser::RENAME:
      case PrestoSqlParser::REPEATABLE:
      case PrestoSqlParser::REPLACE:
      case PrestoSqlParser::RESET:
      case PrestoSqlParser::RESPECT:
      case PrestoSqlParser::RESTRICT:
      case PrestoSqlParser::RETURN:
      case PrestoSqlParser::RETURNS:
      case PrestoSqlParser::REVOKE:
      case PrestoSqlParser::ROLE:
      case PrestoSqlParser::ROLES:
      case PrestoSqlParser::ROLLBACK:
      case PrestoSqlParser::ROW:
      case PrestoSqlParser::ROWS:
      case PrestoSqlParser::SCHEMA:
      case PrestoSqlParser::SCHEMAS:
      case PrestoSqlParser::SECOND:
      case PrestoSqlParser::SECURITY:
      case PrestoSqlParser::SERIALIZABLE:
      case PrestoSqlParser::SESSION:
      case PrestoSqlParser::SET:
      case PrestoSqlParser::SETS:
      case PrestoSqlParser::SHOW:
      case PrestoSqlParser::SOME:
      case PrestoSqlParser::SQL:
      case PrestoSqlParser::START:
      case PrestoSqlParser::STATS:
      case PrestoSqlParser::SUBSTRING:
      case PrestoSqlParser::SYSTEM:
      case PrestoSqlParser::SYSTEM_TIME:
      case PrestoSqlParser::SYSTEM_VERSION:
      case PrestoSqlParser::TABLES:
      case PrestoSqlParser::TABLESAMPLE:
      case PrestoSqlParser::TEMPORARY:
      case PrestoSqlParser::TEXT:
      case PrestoSqlParser::TIME:
      case PrestoSqlParser::TIMESTAMP:
      case PrestoSqlParser::TO:
      case PrestoSqlParser::TRANSACTION:
      case PrestoSqlParser::TRUNCATE:
      case PrestoSqlParser::TRY_CAST:
      case PrestoSqlParser::TYPE:
      case PrestoSqlParser::UNBOUNDED:
      case PrestoSqlParser::UNCOMMITTED:
      case PrestoSqlParser::UNIQUE:
      case PrestoSqlParser::UPDATE:
      case PrestoSqlParser::USE:
      case PrestoSqlParser::USER:
      case PrestoSqlParser::VALIDATE:
      case PrestoSqlParser::VERBOSE:
      case PrestoSqlParser::VERSION:
      case PrestoSqlParser::VIEW:
      case PrestoSqlParser::WORK:
      case PrestoSqlParser::WRITE:
      case PrestoSqlParser::YEAR:
      case PrestoSqlParser::ZONE:
      case PrestoSqlParser::IDENTIFIER:
      case PrestoSqlParser::DIGIT_IDENTIFIER:
      case PrestoSqlParser::QUOTED_IDENTIFIER:
      case PrestoSqlParser::BACKQUOTED_IDENTIFIER: {
        enterOuterAlt(_localctx, 4);
        setState(1967);
        identifier();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QualifiedNameContext
//------------------------------------------------------------------

PrestoSqlParser::QualifiedNameContext::QualifiedNameContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::IdentifierContext*>
PrestoSqlParser::QualifiedNameContext::identifier() {
  return getRuleContexts<PrestoSqlParser::IdentifierContext>();
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::QualifiedNameContext::identifier(size_t i) {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(i);
}

size_t PrestoSqlParser::QualifiedNameContext::getRuleIndex() const {
  return PrestoSqlParser::RuleQualifiedName;
}

void PrestoSqlParser::QualifiedNameContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterQualifiedName(this);
}

void PrestoSqlParser::QualifiedNameContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitQualifiedName(this);
}

antlrcpp::Any PrestoSqlParser::QualifiedNameContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitQualifiedName(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::QualifiedNameContext* PrestoSqlParser::qualifiedName() {
  QualifiedNameContext* _localctx =
      _tracker.createInstance<QualifiedNameContext>(_ctx, getState());
  enterRule(_localctx, 142, PrestoSqlParser::RuleQualifiedName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1970);
    identifier();
    setState(1975);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 248, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1971);
        match(PrestoSqlParser::T__0);
        setState(1972);
        identifier();
      }
      setState(1977);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 248, _ctx);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableVersionExpressionContext
//------------------------------------------------------------------

PrestoSqlParser::TableVersionExpressionContext::TableVersionExpressionContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::TableVersionExpressionContext::getRuleIndex() const {
  return PrestoSqlParser::RuleTableVersionExpression;
}

void PrestoSqlParser::TableVersionExpressionContext::copyFrom(
    TableVersionExpressionContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- TableVersionContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::TableVersionContext::FOR() {
  return getToken(PrestoSqlParser::FOR, 0);
}

PrestoSqlParser::TableVersionStateContext*
PrestoSqlParser::TableVersionContext::tableVersionState() {
  return getRuleContext<PrestoSqlParser::TableVersionStateContext>(0);
}

PrestoSqlParser::ValueExpressionContext*
PrestoSqlParser::TableVersionContext::valueExpression() {
  return getRuleContext<PrestoSqlParser::ValueExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::TableVersionContext::SYSTEM_TIME() {
  return getToken(PrestoSqlParser::SYSTEM_TIME, 0);
}

tree::TerminalNode* PrestoSqlParser::TableVersionContext::SYSTEM_VERSION() {
  return getToken(PrestoSqlParser::SYSTEM_VERSION, 0);
}

tree::TerminalNode* PrestoSqlParser::TableVersionContext::TIMESTAMP() {
  return getToken(PrestoSqlParser::TIMESTAMP, 0);
}

tree::TerminalNode* PrestoSqlParser::TableVersionContext::VERSION() {
  return getToken(PrestoSqlParser::VERSION, 0);
}

PrestoSqlParser::TableVersionContext::TableVersionContext(
    TableVersionExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::TableVersionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTableVersion(this);
}
void PrestoSqlParser::TableVersionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTableVersion(this);
}

antlrcpp::Any PrestoSqlParser::TableVersionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTableVersion(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::TableVersionExpressionContext*
PrestoSqlParser::tableVersionExpression() {
  TableVersionExpressionContext* _localctx =
      _tracker.createInstance<TableVersionExpressionContext>(_ctx, getState());
  enterRule(_localctx, 144, PrestoSqlParser::RuleTableVersionExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    _localctx = _tracker.createInstance<PrestoSqlParser::TableVersionContext>(
        _localctx);
    enterOuterAlt(_localctx, 1);
    setState(1978);
    match(PrestoSqlParser::FOR);
    setState(1979);
    antlrcpp::downCast<TableVersionContext*>(_localctx)->tableVersionType =
        _input->LT(1);
    _la = _input->LA(1);
    if (!(((((_la - 192) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 192)) &
            ((1ULL << (PrestoSqlParser::SYSTEM_TIME - 192)) |
             (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 192)) |
             (1ULL << (PrestoSqlParser::TIMESTAMP - 192)) |
             (1ULL << (PrestoSqlParser::VERSION - 192)))) != 0))) {
      antlrcpp::downCast<TableVersionContext*>(_localctx)->tableVersionType =
          _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(1980);
    tableVersionState();
    setState(1981);
    valueExpression(0);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TableVersionStateContext
//------------------------------------------------------------------

PrestoSqlParser::TableVersionStateContext::TableVersionStateContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::TableVersionStateContext::getRuleIndex() const {
  return PrestoSqlParser::RuleTableVersionState;
}

void PrestoSqlParser::TableVersionStateContext::copyFrom(
    TableVersionStateContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- TableversionbeforeContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::TableversionbeforeContext::BEFORE() {
  return getToken(PrestoSqlParser::BEFORE, 0);
}

PrestoSqlParser::TableversionbeforeContext::TableversionbeforeContext(
    TableVersionStateContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::TableversionbeforeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTableversionbefore(this);
}
void PrestoSqlParser::TableversionbeforeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTableversionbefore(this);
}

antlrcpp::Any PrestoSqlParser::TableversionbeforeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTableversionbefore(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TableversionasofContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::TableversionasofContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

tree::TerminalNode* PrestoSqlParser::TableversionasofContext::OF() {
  return getToken(PrestoSqlParser::OF, 0);
}

PrestoSqlParser::TableversionasofContext::TableversionasofContext(
    TableVersionStateContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::TableversionasofContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTableversionasof(this);
}
void PrestoSqlParser::TableversionasofContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTableversionasof(this);
}

antlrcpp::Any PrestoSqlParser::TableversionasofContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTableversionasof(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::TableVersionStateContext*
PrestoSqlParser::tableVersionState() {
  TableVersionStateContext* _localctx =
      _tracker.createInstance<TableVersionStateContext>(_ctx, getState());
  enterRule(_localctx, 146, PrestoSqlParser::RuleTableVersionState);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1986);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::AS: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TableversionasofContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1983);
        match(PrestoSqlParser::AS);
        setState(1984);
        match(PrestoSqlParser::OF);
        break;
      }

      case PrestoSqlParser::BEFORE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TableversionbeforeContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1985);
        match(PrestoSqlParser::BEFORE);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GrantorContext
//------------------------------------------------------------------

PrestoSqlParser::GrantorContext::GrantorContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::GrantorContext::getRuleIndex() const {
  return PrestoSqlParser::RuleGrantor;
}

void PrestoSqlParser::GrantorContext::copyFrom(GrantorContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- CurrentUserGrantorContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CurrentUserGrantorContext::CURRENT_USER() {
  return getToken(PrestoSqlParser::CURRENT_USER, 0);
}

PrestoSqlParser::CurrentUserGrantorContext::CurrentUserGrantorContext(
    GrantorContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CurrentUserGrantorContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCurrentUserGrantor(this);
}
void PrestoSqlParser::CurrentUserGrantorContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCurrentUserGrantor(this);
}

antlrcpp::Any PrestoSqlParser::CurrentUserGrantorContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCurrentUserGrantor(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SpecifiedPrincipalContext
//------------------------------------------------------------------

PrestoSqlParser::PrincipalContext*
PrestoSqlParser::SpecifiedPrincipalContext::principal() {
  return getRuleContext<PrestoSqlParser::PrincipalContext>(0);
}

PrestoSqlParser::SpecifiedPrincipalContext::SpecifiedPrincipalContext(
    GrantorContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SpecifiedPrincipalContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSpecifiedPrincipal(this);
}
void PrestoSqlParser::SpecifiedPrincipalContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSpecifiedPrincipal(this);
}

antlrcpp::Any PrestoSqlParser::SpecifiedPrincipalContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSpecifiedPrincipal(this);
  else
    return visitor->visitChildren(this);
}
//----------------- CurrentRoleGrantorContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::CurrentRoleGrantorContext::CURRENT_ROLE() {
  return getToken(PrestoSqlParser::CURRENT_ROLE, 0);
}

PrestoSqlParser::CurrentRoleGrantorContext::CurrentRoleGrantorContext(
    GrantorContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::CurrentRoleGrantorContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterCurrentRoleGrantor(this);
}
void PrestoSqlParser::CurrentRoleGrantorContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitCurrentRoleGrantor(this);
}

antlrcpp::Any PrestoSqlParser::CurrentRoleGrantorContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCurrentRoleGrantor(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::GrantorContext* PrestoSqlParser::grantor() {
  GrantorContext* _localctx =
      _tracker.createInstance<GrantorContext>(_ctx, getState());
  enterRule(_localctx, 148, PrestoSqlParser::RuleGrantor);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1991);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 250, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CurrentUserGrantorContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1988);
        match(PrestoSqlParser::CURRENT_USER);
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CurrentRoleGrantorContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1989);
        match(PrestoSqlParser::CURRENT_ROLE);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SpecifiedPrincipalContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(1990);
        principal();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrincipalContext
//------------------------------------------------------------------

PrestoSqlParser::PrincipalContext::PrincipalContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::PrincipalContext::getRuleIndex() const {
  return PrestoSqlParser::RulePrincipal;
}

void PrestoSqlParser::PrincipalContext::copyFrom(PrincipalContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- UnspecifiedPrincipalContext
//------------------------------------------------------------------

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::UnspecifiedPrincipalContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::UnspecifiedPrincipalContext::UnspecifiedPrincipalContext(
    PrincipalContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::UnspecifiedPrincipalContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnspecifiedPrincipal(this);
}
void PrestoSqlParser::UnspecifiedPrincipalContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnspecifiedPrincipal(this);
}

antlrcpp::Any PrestoSqlParser::UnspecifiedPrincipalContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUnspecifiedPrincipal(this);
  else
    return visitor->visitChildren(this);
}
//----------------- UserPrincipalContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::UserPrincipalContext::USER() {
  return getToken(PrestoSqlParser::USER, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::UserPrincipalContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::UserPrincipalContext::UserPrincipalContext(
    PrincipalContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::UserPrincipalContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterUserPrincipal(this);
}
void PrestoSqlParser::UserPrincipalContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitUserPrincipal(this);
}

antlrcpp::Any PrestoSqlParser::UserPrincipalContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUserPrincipal(this);
  else
    return visitor->visitChildren(this);
}
//----------------- RolePrincipalContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::RolePrincipalContext::ROLE() {
  return getToken(PrestoSqlParser::ROLE, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::RolePrincipalContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::RolePrincipalContext::RolePrincipalContext(
    PrincipalContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::RolePrincipalContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRolePrincipal(this);
}
void PrestoSqlParser::RolePrincipalContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRolePrincipal(this);
}

antlrcpp::Any PrestoSqlParser::RolePrincipalContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRolePrincipal(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::PrincipalContext* PrestoSqlParser::principal() {
  PrincipalContext* _localctx =
      _tracker.createInstance<PrincipalContext>(_ctx, getState());
  enterRule(_localctx, 150, PrestoSqlParser::RulePrincipal);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1998);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 251, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UserPrincipalContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1993);
        match(PrestoSqlParser::USER);
        setState(1994);
        identifier();
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RolePrincipalContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1995);
        match(PrestoSqlParser::ROLE);
        setState(1996);
        identifier();
        break;
      }

      case 3: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::UnspecifiedPrincipalContext>(
                    _localctx);
        enterOuterAlt(_localctx, 3);
        setState(1997);
        identifier();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RolesContext
//------------------------------------------------------------------

PrestoSqlParser::RolesContext::RolesContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::IdentifierContext*>
PrestoSqlParser::RolesContext::identifier() {
  return getRuleContexts<PrestoSqlParser::IdentifierContext>();
}

PrestoSqlParser::IdentifierContext* PrestoSqlParser::RolesContext::identifier(
    size_t i) {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(i);
}

size_t PrestoSqlParser::RolesContext::getRuleIndex() const {
  return PrestoSqlParser::RuleRoles;
}

void PrestoSqlParser::RolesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRoles(this);
}

void PrestoSqlParser::RolesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitRoles(this);
}

antlrcpp::Any PrestoSqlParser::RolesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRoles(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::RolesContext* PrestoSqlParser::roles() {
  RolesContext* _localctx =
      _tracker.createInstance<RolesContext>(_ctx, getState());
  enterRule(_localctx, 152, PrestoSqlParser::RuleRoles);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2000);
    identifier();
    setState(2005);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::T__3) {
      setState(2001);
      match(PrestoSqlParser::T__3);
      setState(2002);
      identifier();
      setState(2007);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IdentifierContext
//------------------------------------------------------------------

PrestoSqlParser::IdentifierContext::IdentifierContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::IdentifierContext::getRuleIndex() const {
  return PrestoSqlParser::RuleIdentifier;
}

void PrestoSqlParser::IdentifierContext::copyFrom(IdentifierContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- BackQuotedIdentifierContext
//------------------------------------------------------------------

tree::TerminalNode*
PrestoSqlParser::BackQuotedIdentifierContext::BACKQUOTED_IDENTIFIER() {
  return getToken(PrestoSqlParser::BACKQUOTED_IDENTIFIER, 0);
}

PrestoSqlParser::BackQuotedIdentifierContext::BackQuotedIdentifierContext(
    IdentifierContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::BackQuotedIdentifierContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBackQuotedIdentifier(this);
}
void PrestoSqlParser::BackQuotedIdentifierContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitBackQuotedIdentifier(this);
}

antlrcpp::Any PrestoSqlParser::BackQuotedIdentifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBackQuotedIdentifier(this);
  else
    return visitor->visitChildren(this);
}
//----------------- QuotedIdentifierContext
//------------------------------------------------------------------

tree::TerminalNode*
PrestoSqlParser::QuotedIdentifierContext::QUOTED_IDENTIFIER() {
  return getToken(PrestoSqlParser::QUOTED_IDENTIFIER, 0);
}

PrestoSqlParser::QuotedIdentifierContext::QuotedIdentifierContext(
    IdentifierContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::QuotedIdentifierContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuotedIdentifier(this);
}
void PrestoSqlParser::QuotedIdentifierContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuotedIdentifier(this);
}

antlrcpp::Any PrestoSqlParser::QuotedIdentifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitQuotedIdentifier(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DigitIdentifierContext
//------------------------------------------------------------------

tree::TerminalNode*
PrestoSqlParser::DigitIdentifierContext::DIGIT_IDENTIFIER() {
  return getToken(PrestoSqlParser::DIGIT_IDENTIFIER, 0);
}

PrestoSqlParser::DigitIdentifierContext::DigitIdentifierContext(
    IdentifierContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DigitIdentifierContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDigitIdentifier(this);
}
void PrestoSqlParser::DigitIdentifierContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDigitIdentifier(this);
}

antlrcpp::Any PrestoSqlParser::DigitIdentifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDigitIdentifier(this);
  else
    return visitor->visitChildren(this);
}
//----------------- UnquotedIdentifierContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::UnquotedIdentifierContext::IDENTIFIER() {
  return getToken(PrestoSqlParser::IDENTIFIER, 0);
}

PrestoSqlParser::NonReservedContext*
PrestoSqlParser::UnquotedIdentifierContext::nonReserved() {
  return getRuleContext<PrestoSqlParser::NonReservedContext>(0);
}

PrestoSqlParser::UnquotedIdentifierContext::UnquotedIdentifierContext(
    IdentifierContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::UnquotedIdentifierContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnquotedIdentifier(this);
}
void PrestoSqlParser::UnquotedIdentifierContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnquotedIdentifier(this);
}

antlrcpp::Any PrestoSqlParser::UnquotedIdentifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUnquotedIdentifier(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::IdentifierContext* PrestoSqlParser::identifier() {
  IdentifierContext* _localctx =
      _tracker.createInstance<IdentifierContext>(_ctx, getState());
  enterRule(_localctx, 154, PrestoSqlParser::RuleIdentifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2013);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::IDENTIFIER: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UnquotedIdentifierContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2008);
        match(PrestoSqlParser::IDENTIFIER);
        break;
      }

      case PrestoSqlParser::QUOTED_IDENTIFIER: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::QuotedIdentifierContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2009);
        match(PrestoSqlParser::QUOTED_IDENTIFIER);
        break;
      }

      case PrestoSqlParser::ADD:
      case PrestoSqlParser::ADMIN:
      case PrestoSqlParser::ALL:
      case PrestoSqlParser::ANALYZE:
      case PrestoSqlParser::ANY:
      case PrestoSqlParser::ARRAY:
      case PrestoSqlParser::ASC:
      case PrestoSqlParser::AT:
      case PrestoSqlParser::BEFORE:
      case PrestoSqlParser::BERNOULLI:
      case PrestoSqlParser::CALL:
      case PrestoSqlParser::CALLED:
      case PrestoSqlParser::CASCADE:
      case PrestoSqlParser::CATALOGS:
      case PrestoSqlParser::COLUMN:
      case PrestoSqlParser::COLUMNS:
      case PrestoSqlParser::COMMENT:
      case PrestoSqlParser::COMMIT:
      case PrestoSqlParser::COMMITTED:
      case PrestoSqlParser::CURRENT:
      case PrestoSqlParser::CURRENT_ROLE:
      case PrestoSqlParser::DATA:
      case PrestoSqlParser::DATE:
      case PrestoSqlParser::DAY:
      case PrestoSqlParser::DEFINER:
      case PrestoSqlParser::DESC:
      case PrestoSqlParser::DETERMINISTIC:
      case PrestoSqlParser::DISABLED:
      case PrestoSqlParser::DISTRIBUTED:
      case PrestoSqlParser::ENABLED:
      case PrestoSqlParser::ENFORCED:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXPLAIN:
      case PrestoSqlParser::EXTERNAL:
      case PrestoSqlParser::FETCH:
      case PrestoSqlParser::FILTER:
      case PrestoSqlParser::FIRST:
      case PrestoSqlParser::FOLLOWING:
      case PrestoSqlParser::FORMAT:
      case PrestoSqlParser::FUNCTION:
      case PrestoSqlParser::FUNCTIONS:
      case PrestoSqlParser::GRANT:
      case PrestoSqlParser::GRANTED:
      case PrestoSqlParser::GRANTS:
      case PrestoSqlParser::GRAPHVIZ:
      case PrestoSqlParser::GROUPS:
      case PrestoSqlParser::HOUR:
      case PrestoSqlParser::IF:
      case PrestoSqlParser::IGNORE:
      case PrestoSqlParser::INCLUDING:
      case PrestoSqlParser::INPUT:
      case PrestoSqlParser::INTERVAL:
      case PrestoSqlParser::INVOKER:
      case PrestoSqlParser::IO:
      case PrestoSqlParser::ISOLATION:
      case PrestoSqlParser::JSON:
      case PrestoSqlParser::KEY:
      case PrestoSqlParser::LANGUAGE:
      case PrestoSqlParser::LAST:
      case PrestoSqlParser::LATERAL:
      case PrestoSqlParser::LEVEL:
      case PrestoSqlParser::LIMIT:
      case PrestoSqlParser::LOGICAL:
      case PrestoSqlParser::MAP:
      case PrestoSqlParser::MATERIALIZED:
      case PrestoSqlParser::MINUTE:
      case PrestoSqlParser::MONTH:
      case PrestoSqlParser::NAME:
      case PrestoSqlParser::NFC:
      case PrestoSqlParser::NFD:
      case PrestoSqlParser::NFKC:
      case PrestoSqlParser::NFKD:
      case PrestoSqlParser::NO:
      case PrestoSqlParser::NONE:
      case PrestoSqlParser::NULLIF:
      case PrestoSqlParser::NULLS:
      case PrestoSqlParser::OF:
      case PrestoSqlParser::OFFSET:
      case PrestoSqlParser::ONLY:
      case PrestoSqlParser::OPTION:
      case PrestoSqlParser::ORDINALITY:
      case PrestoSqlParser::OUTPUT:
      case PrestoSqlParser::OVER:
      case PrestoSqlParser::PARTITION:
      case PrestoSqlParser::PARTITIONS:
      case PrestoSqlParser::POSITION:
      case PrestoSqlParser::PRECEDING:
      case PrestoSqlParser::PRIMARY:
      case PrestoSqlParser::PRIVILEGES:
      case PrestoSqlParser::PROPERTIES:
      case PrestoSqlParser::RANGE:
      case PrestoSqlParser::READ:
      case PrestoSqlParser::REFRESH:
      case PrestoSqlParser::RELY:
      case PrestoSqlParser::RENAME:
      case PrestoSqlParser::REPEATABLE:
      case PrestoSqlParser::REPLACE:
      case PrestoSqlParser::RESET:
      case PrestoSqlParser::RESPECT:
      case PrestoSqlParser::RESTRICT:
      case PrestoSqlParser::RETURN:
      case PrestoSqlParser::RETURNS:
      case PrestoSqlParser::REVOKE:
      case PrestoSqlParser::ROLE:
      case PrestoSqlParser::ROLES:
      case PrestoSqlParser::ROLLBACK:
      case PrestoSqlParser::ROW:
      case PrestoSqlParser::ROWS:
      case PrestoSqlParser::SCHEMA:
      case PrestoSqlParser::SCHEMAS:
      case PrestoSqlParser::SECOND:
      case PrestoSqlParser::SECURITY:
      case PrestoSqlParser::SERIALIZABLE:
      case PrestoSqlParser::SESSION:
      case PrestoSqlParser::SET:
      case PrestoSqlParser::SETS:
      case PrestoSqlParser::SHOW:
      case PrestoSqlParser::SOME:
      case PrestoSqlParser::SQL:
      case PrestoSqlParser::START:
      case PrestoSqlParser::STATS:
      case PrestoSqlParser::SUBSTRING:
      case PrestoSqlParser::SYSTEM:
      case PrestoSqlParser::SYSTEM_TIME:
      case PrestoSqlParser::SYSTEM_VERSION:
      case PrestoSqlParser::TABLES:
      case PrestoSqlParser::TABLESAMPLE:
      case PrestoSqlParser::TEMPORARY:
      case PrestoSqlParser::TEXT:
      case PrestoSqlParser::TIME:
      case PrestoSqlParser::TIMESTAMP:
      case PrestoSqlParser::TO:
      case PrestoSqlParser::TRANSACTION:
      case PrestoSqlParser::TRUNCATE:
      case PrestoSqlParser::TRY_CAST:
      case PrestoSqlParser::TYPE:
      case PrestoSqlParser::UNBOUNDED:
      case PrestoSqlParser::UNCOMMITTED:
      case PrestoSqlParser::UNIQUE:
      case PrestoSqlParser::UPDATE:
      case PrestoSqlParser::USE:
      case PrestoSqlParser::USER:
      case PrestoSqlParser::VALIDATE:
      case PrestoSqlParser::VERBOSE:
      case PrestoSqlParser::VERSION:
      case PrestoSqlParser::VIEW:
      case PrestoSqlParser::WORK:
      case PrestoSqlParser::WRITE:
      case PrestoSqlParser::YEAR:
      case PrestoSqlParser::ZONE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UnquotedIdentifierContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(2010);
        nonReserved();
        break;
      }

      case PrestoSqlParser::BACKQUOTED_IDENTIFIER: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::BackQuotedIdentifierContext>(
                    _localctx);
        enterOuterAlt(_localctx, 4);
        setState(2011);
        match(PrestoSqlParser::BACKQUOTED_IDENTIFIER);
        break;
      }

      case PrestoSqlParser::DIGIT_IDENTIFIER: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DigitIdentifierContext>(
                _localctx);
        enterOuterAlt(_localctx, 5);
        setState(2012);
        match(PrestoSqlParser::DIGIT_IDENTIFIER);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumberContext
//------------------------------------------------------------------

PrestoSqlParser::NumberContext::NumberContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t PrestoSqlParser::NumberContext::getRuleIndex() const {
  return PrestoSqlParser::RuleNumber;
}

void PrestoSqlParser::NumberContext::copyFrom(NumberContext* ctx) {
  ParserRuleContext::copyFrom(ctx);
}

//----------------- DecimalLiteralContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DecimalLiteralContext::DECIMAL_VALUE() {
  return getToken(PrestoSqlParser::DECIMAL_VALUE, 0);
}

PrestoSqlParser::DecimalLiteralContext::DecimalLiteralContext(
    NumberContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DecimalLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDecimalLiteral(this);
}
void PrestoSqlParser::DecimalLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDecimalLiteral(this);
}

antlrcpp::Any PrestoSqlParser::DecimalLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDecimalLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- DoubleLiteralContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::DoubleLiteralContext::DOUBLE_VALUE() {
  return getToken(PrestoSqlParser::DOUBLE_VALUE, 0);
}

PrestoSqlParser::DoubleLiteralContext::DoubleLiteralContext(
    NumberContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::DoubleLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterDoubleLiteral(this);
}
void PrestoSqlParser::DoubleLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitDoubleLiteral(this);
}

antlrcpp::Any PrestoSqlParser::DoubleLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDoubleLiteral(this);
  else
    return visitor->visitChildren(this);
}
//----------------- IntegerLiteralContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::IntegerLiteralContext::INTEGER_VALUE() {
  return getToken(PrestoSqlParser::INTEGER_VALUE, 0);
}

PrestoSqlParser::IntegerLiteralContext::IntegerLiteralContext(
    NumberContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::IntegerLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterIntegerLiteral(this);
}
void PrestoSqlParser::IntegerLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitIntegerLiteral(this);
}

antlrcpp::Any PrestoSqlParser::IntegerLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitIntegerLiteral(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::NumberContext* PrestoSqlParser::number() {
  NumberContext* _localctx =
      _tracker.createInstance<NumberContext>(_ctx, getState());
  enterRule(_localctx, 156, PrestoSqlParser::RuleNumber);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2018);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::DECIMAL_VALUE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DecimalLiteralContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2015);
        match(PrestoSqlParser::DECIMAL_VALUE);
        break;
      }

      case PrestoSqlParser::DOUBLE_VALUE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DoubleLiteralContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2016);
        match(PrestoSqlParser::DOUBLE_VALUE);
        break;
      }

      case PrestoSqlParser::INTEGER_VALUE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::IntegerLiteralContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(2017);
        match(PrestoSqlParser::INTEGER_VALUE);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstraintSpecificationContext
//------------------------------------------------------------------

PrestoSqlParser::ConstraintSpecificationContext::ConstraintSpecificationContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::NamedConstraintSpecificationContext* PrestoSqlParser::
    ConstraintSpecificationContext::namedConstraintSpecification() {
  return getRuleContext<PrestoSqlParser::NamedConstraintSpecificationContext>(
      0);
}

PrestoSqlParser::UnnamedConstraintSpecificationContext* PrestoSqlParser::
    ConstraintSpecificationContext::unnamedConstraintSpecification() {
  return getRuleContext<PrestoSqlParser::UnnamedConstraintSpecificationContext>(
      0);
}

size_t PrestoSqlParser::ConstraintSpecificationContext::getRuleIndex() const {
  return PrestoSqlParser::RuleConstraintSpecification;
}

void PrestoSqlParser::ConstraintSpecificationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstraintSpecification(this);
}

void PrestoSqlParser::ConstraintSpecificationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstraintSpecification(this);
}

antlrcpp::Any PrestoSqlParser::ConstraintSpecificationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitConstraintSpecification(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ConstraintSpecificationContext*
PrestoSqlParser::constraintSpecification() {
  ConstraintSpecificationContext* _localctx =
      _tracker.createInstance<ConstraintSpecificationContext>(_ctx, getState());
  enterRule(_localctx, 158, PrestoSqlParser::RuleConstraintSpecification);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2022);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::CONSTRAINT: {
        enterOuterAlt(_localctx, 1);
        setState(2020);
        namedConstraintSpecification();
        break;
      }

      case PrestoSqlParser::PRIMARY:
      case PrestoSqlParser::UNIQUE: {
        enterOuterAlt(_localctx, 2);
        setState(2021);
        unnamedConstraintSpecification();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NamedConstraintSpecificationContext
//------------------------------------------------------------------

PrestoSqlParser::NamedConstraintSpecificationContext::
    NamedConstraintSpecificationContext(
        ParserRuleContext* parent,
        size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
PrestoSqlParser::NamedConstraintSpecificationContext::CONSTRAINT() {
  return getToken(PrestoSqlParser::CONSTRAINT, 0);
}

PrestoSqlParser::UnnamedConstraintSpecificationContext* PrestoSqlParser::
    NamedConstraintSpecificationContext::unnamedConstraintSpecification() {
  return getRuleContext<PrestoSqlParser::UnnamedConstraintSpecificationContext>(
      0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::NamedConstraintSpecificationContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

size_t PrestoSqlParser::NamedConstraintSpecificationContext::getRuleIndex()
    const {
  return PrestoSqlParser::RuleNamedConstraintSpecification;
}

void PrestoSqlParser::NamedConstraintSpecificationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNamedConstraintSpecification(this);
}

void PrestoSqlParser::NamedConstraintSpecificationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNamedConstraintSpecification(this);
}

antlrcpp::Any PrestoSqlParser::NamedConstraintSpecificationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNamedConstraintSpecification(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::NamedConstraintSpecificationContext*
PrestoSqlParser::namedConstraintSpecification() {
  NamedConstraintSpecificationContext* _localctx =
      _tracker.createInstance<NamedConstraintSpecificationContext>(
          _ctx, getState());
  enterRule(_localctx, 160, PrestoSqlParser::RuleNamedConstraintSpecification);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2024);
    match(PrestoSqlParser::CONSTRAINT);
    setState(2025);
    antlrcpp::downCast<NamedConstraintSpecificationContext*>(_localctx)->name =
        identifier();
    setState(2026);
    unnamedConstraintSpecification();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- UnnamedConstraintSpecificationContext
//------------------------------------------------------------------

PrestoSqlParser::UnnamedConstraintSpecificationContext::
    UnnamedConstraintSpecificationContext(
        ParserRuleContext* parent,
        size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::ConstraintTypeContext*
PrestoSqlParser::UnnamedConstraintSpecificationContext::constraintType() {
  return getRuleContext<PrestoSqlParser::ConstraintTypeContext>(0);
}

PrestoSqlParser::ColumnAliasesContext*
PrestoSqlParser::UnnamedConstraintSpecificationContext::columnAliases() {
  return getRuleContext<PrestoSqlParser::ColumnAliasesContext>(0);
}

PrestoSqlParser::ConstraintQualifiersContext*
PrestoSqlParser::UnnamedConstraintSpecificationContext::constraintQualifiers() {
  return getRuleContext<PrestoSqlParser::ConstraintQualifiersContext>(0);
}

size_t PrestoSqlParser::UnnamedConstraintSpecificationContext::getRuleIndex()
    const {
  return PrestoSqlParser::RuleUnnamedConstraintSpecification;
}

void PrestoSqlParser::UnnamedConstraintSpecificationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnnamedConstraintSpecification(this);
}

void PrestoSqlParser::UnnamedConstraintSpecificationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnnamedConstraintSpecification(this);
}

antlrcpp::Any PrestoSqlParser::UnnamedConstraintSpecificationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUnnamedConstraintSpecification(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::UnnamedConstraintSpecificationContext*
PrestoSqlParser::unnamedConstraintSpecification() {
  UnnamedConstraintSpecificationContext* _localctx =
      _tracker.createInstance<UnnamedConstraintSpecificationContext>(
          _ctx, getState());
  enterRule(
      _localctx, 162, PrestoSqlParser::RuleUnnamedConstraintSpecification);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2028);
    constraintType();
    setState(2029);
    columnAliases();
    setState(2031);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 256, _ctx)) {
      case 1: {
        setState(2030);
        constraintQualifiers();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstraintTypeContext
//------------------------------------------------------------------

PrestoSqlParser::ConstraintTypeContext::ConstraintTypeContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::ConstraintTypeContext::UNIQUE() {
  return getToken(PrestoSqlParser::UNIQUE, 0);
}

tree::TerminalNode* PrestoSqlParser::ConstraintTypeContext::PRIMARY() {
  return getToken(PrestoSqlParser::PRIMARY, 0);
}

tree::TerminalNode* PrestoSqlParser::ConstraintTypeContext::KEY() {
  return getToken(PrestoSqlParser::KEY, 0);
}

size_t PrestoSqlParser::ConstraintTypeContext::getRuleIndex() const {
  return PrestoSqlParser::RuleConstraintType;
}

void PrestoSqlParser::ConstraintTypeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstraintType(this);
}

void PrestoSqlParser::ConstraintTypeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstraintType(this);
}

antlrcpp::Any PrestoSqlParser::ConstraintTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitConstraintType(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ConstraintTypeContext* PrestoSqlParser::constraintType() {
  ConstraintTypeContext* _localctx =
      _tracker.createInstance<ConstraintTypeContext>(_ctx, getState());
  enterRule(_localctx, 164, PrestoSqlParser::RuleConstraintType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2036);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::UNIQUE: {
        enterOuterAlt(_localctx, 1);
        setState(2033);
        match(PrestoSqlParser::UNIQUE);
        break;
      }

      case PrestoSqlParser::PRIMARY: {
        enterOuterAlt(_localctx, 2);
        setState(2034);
        match(PrestoSqlParser::PRIMARY);
        setState(2035);
        match(PrestoSqlParser::KEY);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstraintQualifiersContext
//------------------------------------------------------------------

PrestoSqlParser::ConstraintQualifiersContext::ConstraintQualifiersContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::ConstraintQualifierContext*>
PrestoSqlParser::ConstraintQualifiersContext::constraintQualifier() {
  return getRuleContexts<PrestoSqlParser::ConstraintQualifierContext>();
}

PrestoSqlParser::ConstraintQualifierContext*
PrestoSqlParser::ConstraintQualifiersContext::constraintQualifier(size_t i) {
  return getRuleContext<PrestoSqlParser::ConstraintQualifierContext>(i);
}

size_t PrestoSqlParser::ConstraintQualifiersContext::getRuleIndex() const {
  return PrestoSqlParser::RuleConstraintQualifiers;
}

void PrestoSqlParser::ConstraintQualifiersContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstraintQualifiers(this);
}

void PrestoSqlParser::ConstraintQualifiersContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstraintQualifiers(this);
}

antlrcpp::Any PrestoSqlParser::ConstraintQualifiersContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitConstraintQualifiers(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ConstraintQualifiersContext*
PrestoSqlParser::constraintQualifiers() {
  ConstraintQualifiersContext* _localctx =
      _tracker.createInstance<ConstraintQualifiersContext>(_ctx, getState());
  enterRule(_localctx, 166, PrestoSqlParser::RuleConstraintQualifiers);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2041);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while ((((_la & ~0x3fULL) == 0) &&
            ((1ULL << _la) &
             ((1ULL << PrestoSqlParser::DISABLED) |
              (1ULL << PrestoSqlParser::ENABLED) |
              (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
           _la == PrestoSqlParser::NOT

           || _la == PrestoSqlParser::RELY) {
      setState(2038);
      constraintQualifier();
      setState(2043);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstraintQualifierContext
//------------------------------------------------------------------

PrestoSqlParser::ConstraintQualifierContext::ConstraintQualifierContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::ConstraintEnabledContext*
PrestoSqlParser::ConstraintQualifierContext::constraintEnabled() {
  return getRuleContext<PrestoSqlParser::ConstraintEnabledContext>(0);
}

PrestoSqlParser::ConstraintRelyContext*
PrestoSqlParser::ConstraintQualifierContext::constraintRely() {
  return getRuleContext<PrestoSqlParser::ConstraintRelyContext>(0);
}

PrestoSqlParser::ConstraintEnforcedContext*
PrestoSqlParser::ConstraintQualifierContext::constraintEnforced() {
  return getRuleContext<PrestoSqlParser::ConstraintEnforcedContext>(0);
}

size_t PrestoSqlParser::ConstraintQualifierContext::getRuleIndex() const {
  return PrestoSqlParser::RuleConstraintQualifier;
}

void PrestoSqlParser::ConstraintQualifierContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstraintQualifier(this);
}

void PrestoSqlParser::ConstraintQualifierContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstraintQualifier(this);
}

antlrcpp::Any PrestoSqlParser::ConstraintQualifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitConstraintQualifier(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ConstraintQualifierContext*
PrestoSqlParser::constraintQualifier() {
  ConstraintQualifierContext* _localctx =
      _tracker.createInstance<ConstraintQualifierContext>(_ctx, getState());
  enterRule(_localctx, 168, PrestoSqlParser::RuleConstraintQualifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2047);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 259, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(2044);
        constraintEnabled();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(2045);
        constraintRely();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(2046);
        constraintEnforced();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstraintRelyContext
//------------------------------------------------------------------

PrestoSqlParser::ConstraintRelyContext::ConstraintRelyContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::ConstraintRelyContext::RELY() {
  return getToken(PrestoSqlParser::RELY, 0);
}

tree::TerminalNode* PrestoSqlParser::ConstraintRelyContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

size_t PrestoSqlParser::ConstraintRelyContext::getRuleIndex() const {
  return PrestoSqlParser::RuleConstraintRely;
}

void PrestoSqlParser::ConstraintRelyContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstraintRely(this);
}

void PrestoSqlParser::ConstraintRelyContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstraintRely(this);
}

antlrcpp::Any PrestoSqlParser::ConstraintRelyContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitConstraintRely(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ConstraintRelyContext* PrestoSqlParser::constraintRely() {
  ConstraintRelyContext* _localctx =
      _tracker.createInstance<ConstraintRelyContext>(_ctx, getState());
  enterRule(_localctx, 170, PrestoSqlParser::RuleConstraintRely);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2052);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::RELY: {
        enterOuterAlt(_localctx, 1);
        setState(2049);
        match(PrestoSqlParser::RELY);
        break;
      }

      case PrestoSqlParser::NOT: {
        enterOuterAlt(_localctx, 2);
        setState(2050);
        match(PrestoSqlParser::NOT);
        setState(2051);
        match(PrestoSqlParser::RELY);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstraintEnabledContext
//------------------------------------------------------------------

PrestoSqlParser::ConstraintEnabledContext::ConstraintEnabledContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::ConstraintEnabledContext::ENABLED() {
  return getToken(PrestoSqlParser::ENABLED, 0);
}

tree::TerminalNode* PrestoSqlParser::ConstraintEnabledContext::DISABLED() {
  return getToken(PrestoSqlParser::DISABLED, 0);
}

size_t PrestoSqlParser::ConstraintEnabledContext::getRuleIndex() const {
  return PrestoSqlParser::RuleConstraintEnabled;
}

void PrestoSqlParser::ConstraintEnabledContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstraintEnabled(this);
}

void PrestoSqlParser::ConstraintEnabledContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstraintEnabled(this);
}

antlrcpp::Any PrestoSqlParser::ConstraintEnabledContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitConstraintEnabled(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ConstraintEnabledContext*
PrestoSqlParser::constraintEnabled() {
  ConstraintEnabledContext* _localctx =
      _tracker.createInstance<ConstraintEnabledContext>(_ctx, getState());
  enterRule(_localctx, 172, PrestoSqlParser::RuleConstraintEnabled);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2054);
    _la = _input->LA(1);
    if (!(_la == PrestoSqlParser::DISABLED

          || _la == PrestoSqlParser::ENABLED)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstraintEnforcedContext
//------------------------------------------------------------------

PrestoSqlParser::ConstraintEnforcedContext::ConstraintEnforcedContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::ConstraintEnforcedContext::ENFORCED() {
  return getToken(PrestoSqlParser::ENFORCED, 0);
}

tree::TerminalNode* PrestoSqlParser::ConstraintEnforcedContext::NOT() {
  return getToken(PrestoSqlParser::NOT, 0);
}

size_t PrestoSqlParser::ConstraintEnforcedContext::getRuleIndex() const {
  return PrestoSqlParser::RuleConstraintEnforced;
}

void PrestoSqlParser::ConstraintEnforcedContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstraintEnforced(this);
}

void PrestoSqlParser::ConstraintEnforcedContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstraintEnforced(this);
}

antlrcpp::Any PrestoSqlParser::ConstraintEnforcedContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitConstraintEnforced(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ConstraintEnforcedContext*
PrestoSqlParser::constraintEnforced() {
  ConstraintEnforcedContext* _localctx =
      _tracker.createInstance<ConstraintEnforcedContext>(_ctx, getState());
  enterRule(_localctx, 174, PrestoSqlParser::RuleConstraintEnforced);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2059);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::ENFORCED: {
        enterOuterAlt(_localctx, 1);
        setState(2056);
        match(PrestoSqlParser::ENFORCED);
        break;
      }

      case PrestoSqlParser::NOT: {
        enterOuterAlt(_localctx, 2);
        setState(2057);
        match(PrestoSqlParser::NOT);
        setState(2058);
        match(PrestoSqlParser::ENFORCED);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NonReservedContext
//------------------------------------------------------------------

PrestoSqlParser::NonReservedContext::NonReservedContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ADD() {
  return getToken(PrestoSqlParser::ADD, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ADMIN() {
  return getToken(PrestoSqlParser::ADMIN, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ALL() {
  return getToken(PrestoSqlParser::ALL, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ANALYZE() {
  return getToken(PrestoSqlParser::ANALYZE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ANY() {
  return getToken(PrestoSqlParser::ANY, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ARRAY() {
  return getToken(PrestoSqlParser::ARRAY, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ASC() {
  return getToken(PrestoSqlParser::ASC, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::AT() {
  return getToken(PrestoSqlParser::AT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::BEFORE() {
  return getToken(PrestoSqlParser::BEFORE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::BERNOULLI() {
  return getToken(PrestoSqlParser::BERNOULLI, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::CALL() {
  return getToken(PrestoSqlParser::CALL, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::CALLED() {
  return getToken(PrestoSqlParser::CALLED, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::CASCADE() {
  return getToken(PrestoSqlParser::CASCADE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::CATALOGS() {
  return getToken(PrestoSqlParser::CATALOGS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::COLUMN() {
  return getToken(PrestoSqlParser::COLUMN, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::COLUMNS() {
  return getToken(PrestoSqlParser::COLUMNS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::COMMENT() {
  return getToken(PrestoSqlParser::COMMENT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::COMMIT() {
  return getToken(PrestoSqlParser::COMMIT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::COMMITTED() {
  return getToken(PrestoSqlParser::COMMITTED, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::CURRENT() {
  return getToken(PrestoSqlParser::CURRENT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::CURRENT_ROLE() {
  return getToken(PrestoSqlParser::CURRENT_ROLE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::DATA() {
  return getToken(PrestoSqlParser::DATA, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::DATE() {
  return getToken(PrestoSqlParser::DATE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::DAY() {
  return getToken(PrestoSqlParser::DAY, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::DEFINER() {
  return getToken(PrestoSqlParser::DEFINER, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::DESC() {
  return getToken(PrestoSqlParser::DESC, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::DETERMINISTIC() {
  return getToken(PrestoSqlParser::DETERMINISTIC, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::DISABLED() {
  return getToken(PrestoSqlParser::DISABLED, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::DISTRIBUTED() {
  return getToken(PrestoSqlParser::DISTRIBUTED, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ENABLED() {
  return getToken(PrestoSqlParser::ENABLED, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ENFORCED() {
  return getToken(PrestoSqlParser::ENFORCED, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::EXCLUDING() {
  return getToken(PrestoSqlParser::EXCLUDING, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::EXPLAIN() {
  return getToken(PrestoSqlParser::EXPLAIN, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::EXTERNAL() {
  return getToken(PrestoSqlParser::EXTERNAL, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::FETCH() {
  return getToken(PrestoSqlParser::FETCH, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::FILTER() {
  return getToken(PrestoSqlParser::FILTER, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::FIRST() {
  return getToken(PrestoSqlParser::FIRST, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::FOLLOWING() {
  return getToken(PrestoSqlParser::FOLLOWING, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::FORMAT() {
  return getToken(PrestoSqlParser::FORMAT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::FUNCTION() {
  return getToken(PrestoSqlParser::FUNCTION, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::FUNCTIONS() {
  return getToken(PrestoSqlParser::FUNCTIONS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::GRANT() {
  return getToken(PrestoSqlParser::GRANT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::GRANTED() {
  return getToken(PrestoSqlParser::GRANTED, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::GRANTS() {
  return getToken(PrestoSqlParser::GRANTS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::GRAPHVIZ() {
  return getToken(PrestoSqlParser::GRAPHVIZ, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::GROUPS() {
  return getToken(PrestoSqlParser::GROUPS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::HOUR() {
  return getToken(PrestoSqlParser::HOUR, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::IF() {
  return getToken(PrestoSqlParser::IF, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::IGNORE() {
  return getToken(PrestoSqlParser::IGNORE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::INCLUDING() {
  return getToken(PrestoSqlParser::INCLUDING, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::INPUT() {
  return getToken(PrestoSqlParser::INPUT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::INTERVAL() {
  return getToken(PrestoSqlParser::INTERVAL, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::INVOKER() {
  return getToken(PrestoSqlParser::INVOKER, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::IO() {
  return getToken(PrestoSqlParser::IO, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ISOLATION() {
  return getToken(PrestoSqlParser::ISOLATION, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::JSON() {
  return getToken(PrestoSqlParser::JSON, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::KEY() {
  return getToken(PrestoSqlParser::KEY, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::LANGUAGE() {
  return getToken(PrestoSqlParser::LANGUAGE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::LAST() {
  return getToken(PrestoSqlParser::LAST, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::LATERAL() {
  return getToken(PrestoSqlParser::LATERAL, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::LEVEL() {
  return getToken(PrestoSqlParser::LEVEL, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::LIMIT() {
  return getToken(PrestoSqlParser::LIMIT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::LOGICAL() {
  return getToken(PrestoSqlParser::LOGICAL, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::MAP() {
  return getToken(PrestoSqlParser::MAP, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::MATERIALIZED() {
  return getToken(PrestoSqlParser::MATERIALIZED, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::MINUTE() {
  return getToken(PrestoSqlParser::MINUTE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::MONTH() {
  return getToken(PrestoSqlParser::MONTH, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::NAME() {
  return getToken(PrestoSqlParser::NAME, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::NFC() {
  return getToken(PrestoSqlParser::NFC, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::NFD() {
  return getToken(PrestoSqlParser::NFD, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::NFKC() {
  return getToken(PrestoSqlParser::NFKC, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::NFKD() {
  return getToken(PrestoSqlParser::NFKD, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::NO() {
  return getToken(PrestoSqlParser::NO, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::NONE() {
  return getToken(PrestoSqlParser::NONE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::NULLIF() {
  return getToken(PrestoSqlParser::NULLIF, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::NULLS() {
  return getToken(PrestoSqlParser::NULLS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::OF() {
  return getToken(PrestoSqlParser::OF, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::OFFSET() {
  return getToken(PrestoSqlParser::OFFSET, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ONLY() {
  return getToken(PrestoSqlParser::ONLY, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::OPTION() {
  return getToken(PrestoSqlParser::OPTION, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ORDINALITY() {
  return getToken(PrestoSqlParser::ORDINALITY, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::OUTPUT() {
  return getToken(PrestoSqlParser::OUTPUT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::OVER() {
  return getToken(PrestoSqlParser::OVER, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::PARTITION() {
  return getToken(PrestoSqlParser::PARTITION, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::PARTITIONS() {
  return getToken(PrestoSqlParser::PARTITIONS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::POSITION() {
  return getToken(PrestoSqlParser::POSITION, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::PRECEDING() {
  return getToken(PrestoSqlParser::PRECEDING, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::PRIMARY() {
  return getToken(PrestoSqlParser::PRIMARY, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::PRIVILEGES() {
  return getToken(PrestoSqlParser::PRIVILEGES, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::PROPERTIES() {
  return getToken(PrestoSqlParser::PROPERTIES, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::RANGE() {
  return getToken(PrestoSqlParser::RANGE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::READ() {
  return getToken(PrestoSqlParser::READ, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::REFRESH() {
  return getToken(PrestoSqlParser::REFRESH, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::RELY() {
  return getToken(PrestoSqlParser::RELY, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::RENAME() {
  return getToken(PrestoSqlParser::RENAME, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::REPEATABLE() {
  return getToken(PrestoSqlParser::REPEATABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::REPLACE() {
  return getToken(PrestoSqlParser::REPLACE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::RESET() {
  return getToken(PrestoSqlParser::RESET, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::RESPECT() {
  return getToken(PrestoSqlParser::RESPECT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::RESTRICT() {
  return getToken(PrestoSqlParser::RESTRICT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::RETURN() {
  return getToken(PrestoSqlParser::RETURN, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::RETURNS() {
  return getToken(PrestoSqlParser::RETURNS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::REVOKE() {
  return getToken(PrestoSqlParser::REVOKE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ROLE() {
  return getToken(PrestoSqlParser::ROLE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ROLES() {
  return getToken(PrestoSqlParser::ROLES, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ROLLBACK() {
  return getToken(PrestoSqlParser::ROLLBACK, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ROW() {
  return getToken(PrestoSqlParser::ROW, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ROWS() {
  return getToken(PrestoSqlParser::ROWS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SCHEMA() {
  return getToken(PrestoSqlParser::SCHEMA, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SCHEMAS() {
  return getToken(PrestoSqlParser::SCHEMAS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SECOND() {
  return getToken(PrestoSqlParser::SECOND, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SECURITY() {
  return getToken(PrestoSqlParser::SECURITY, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SERIALIZABLE() {
  return getToken(PrestoSqlParser::SERIALIZABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SESSION() {
  return getToken(PrestoSqlParser::SESSION, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SET() {
  return getToken(PrestoSqlParser::SET, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SETS() {
  return getToken(PrestoSqlParser::SETS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SQL() {
  return getToken(PrestoSqlParser::SQL, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SHOW() {
  return getToken(PrestoSqlParser::SHOW, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SOME() {
  return getToken(PrestoSqlParser::SOME, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::START() {
  return getToken(PrestoSqlParser::START, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::STATS() {
  return getToken(PrestoSqlParser::STATS, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SUBSTRING() {
  return getToken(PrestoSqlParser::SUBSTRING, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SYSTEM() {
  return getToken(PrestoSqlParser::SYSTEM, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SYSTEM_TIME() {
  return getToken(PrestoSqlParser::SYSTEM_TIME, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::SYSTEM_VERSION() {
  return getToken(PrestoSqlParser::SYSTEM_VERSION, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TABLES() {
  return getToken(PrestoSqlParser::TABLES, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TABLESAMPLE() {
  return getToken(PrestoSqlParser::TABLESAMPLE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TEMPORARY() {
  return getToken(PrestoSqlParser::TEMPORARY, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TEXT() {
  return getToken(PrestoSqlParser::TEXT, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TIME() {
  return getToken(PrestoSqlParser::TIME, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TIMESTAMP() {
  return getToken(PrestoSqlParser::TIMESTAMP, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TO() {
  return getToken(PrestoSqlParser::TO, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TRANSACTION() {
  return getToken(PrestoSqlParser::TRANSACTION, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TRUNCATE() {
  return getToken(PrestoSqlParser::TRUNCATE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TRY_CAST() {
  return getToken(PrestoSqlParser::TRY_CAST, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::TYPE() {
  return getToken(PrestoSqlParser::TYPE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::UNBOUNDED() {
  return getToken(PrestoSqlParser::UNBOUNDED, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::UNCOMMITTED() {
  return getToken(PrestoSqlParser::UNCOMMITTED, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::UNIQUE() {
  return getToken(PrestoSqlParser::UNIQUE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::UPDATE() {
  return getToken(PrestoSqlParser::UPDATE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::USE() {
  return getToken(PrestoSqlParser::USE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::USER() {
  return getToken(PrestoSqlParser::USER, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::VALIDATE() {
  return getToken(PrestoSqlParser::VALIDATE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::VERBOSE() {
  return getToken(PrestoSqlParser::VERBOSE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::VERSION() {
  return getToken(PrestoSqlParser::VERSION, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::VIEW() {
  return getToken(PrestoSqlParser::VIEW, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::WORK() {
  return getToken(PrestoSqlParser::WORK, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::WRITE() {
  return getToken(PrestoSqlParser::WRITE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::YEAR() {
  return getToken(PrestoSqlParser::YEAR, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::ZONE() {
  return getToken(PrestoSqlParser::ZONE, 0);
}

size_t PrestoSqlParser::NonReservedContext::getRuleIndex() const {
  return PrestoSqlParser::RuleNonReserved;
}

void PrestoSqlParser::NonReservedContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNonReserved(this);
}

void PrestoSqlParser::NonReservedContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNonReserved(this);
}

antlrcpp::Any PrestoSqlParser::NonReservedContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNonReserved(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::NonReservedContext* PrestoSqlParser::nonReserved() {
  NonReservedContext* _localctx =
      _tracker.createInstance<NonReservedContext>(_ctx, getState());
  enterRule(_localctx, 176, PrestoSqlParser::RuleNonReserved);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2061);
    _la = _input->LA(1);
    if (!((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) &
            ((1ULL << PrestoSqlParser::ADD) | (1ULL << PrestoSqlParser::ADMIN) |
             (1ULL << PrestoSqlParser::ALL) |
             (1ULL << PrestoSqlParser::ANALYZE) |
             (1ULL << PrestoSqlParser::ANY) | (1ULL << PrestoSqlParser::ARRAY) |
             (1ULL << PrestoSqlParser::ASC) | (1ULL << PrestoSqlParser::AT) |
             (1ULL << PrestoSqlParser::BEFORE) |
             (1ULL << PrestoSqlParser::BERNOULLI) |
             (1ULL << PrestoSqlParser::CALL) |
             (1ULL << PrestoSqlParser::CALLED) |
             (1ULL << PrestoSqlParser::CASCADE) |
             (1ULL << PrestoSqlParser::CATALOGS) |
             (1ULL << PrestoSqlParser::COLUMN) |
             (1ULL << PrestoSqlParser::COLUMNS) |
             (1ULL << PrestoSqlParser::COMMENT) |
             (1ULL << PrestoSqlParser::COMMIT) |
             (1ULL << PrestoSqlParser::COMMITTED) |
             (1ULL << PrestoSqlParser::CURRENT) |
             (1ULL << PrestoSqlParser::CURRENT_ROLE) |
             (1ULL << PrestoSqlParser::DATA) | (1ULL << PrestoSqlParser::DATE) |
             (1ULL << PrestoSqlParser::DAY) |
             (1ULL << PrestoSqlParser::DEFINER) |
             (1ULL << PrestoSqlParser::DESC) |
             (1ULL << PrestoSqlParser::DETERMINISTIC) |
             (1ULL << PrestoSqlParser::DISABLED) |
             (1ULL << PrestoSqlParser::DISTRIBUTED) |
             (1ULL << PrestoSqlParser::ENABLED) |
             (1ULL << PrestoSqlParser::ENFORCED))) != 0) ||
          ((((_la - 66) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 66)) &
            ((1ULL << (PrestoSqlParser::EXCLUDING - 66)) |
             (1ULL << (PrestoSqlParser::EXPLAIN - 66)) |
             (1ULL << (PrestoSqlParser::EXTERNAL - 66)) |
             (1ULL << (PrestoSqlParser::FETCH - 66)) |
             (1ULL << (PrestoSqlParser::FILTER - 66)) |
             (1ULL << (PrestoSqlParser::FIRST - 66)) |
             (1ULL << (PrestoSqlParser::FOLLOWING - 66)) |
             (1ULL << (PrestoSqlParser::FORMAT - 66)) |
             (1ULL << (PrestoSqlParser::FUNCTION - 66)) |
             (1ULL << (PrestoSqlParser::FUNCTIONS - 66)) |
             (1ULL << (PrestoSqlParser::GRANT - 66)) |
             (1ULL << (PrestoSqlParser::GRANTED - 66)) |
             (1ULL << (PrestoSqlParser::GRANTS - 66)) |
             (1ULL << (PrestoSqlParser::GRAPHVIZ - 66)) |
             (1ULL << (PrestoSqlParser::GROUPS - 66)) |
             (1ULL << (PrestoSqlParser::HOUR - 66)) |
             (1ULL << (PrestoSqlParser::IF - 66)) |
             (1ULL << (PrestoSqlParser::IGNORE - 66)) |
             (1ULL << (PrestoSqlParser::INCLUDING - 66)) |
             (1ULL << (PrestoSqlParser::INPUT - 66)) |
             (1ULL << (PrestoSqlParser::INTERVAL - 66)) |
             (1ULL << (PrestoSqlParser::INVOKER - 66)) |
             (1ULL << (PrestoSqlParser::IO - 66)) |
             (1ULL << (PrestoSqlParser::ISOLATION - 66)) |
             (1ULL << (PrestoSqlParser::JSON - 66)) |
             (1ULL << (PrestoSqlParser::KEY - 66)) |
             (1ULL << (PrestoSqlParser::LANGUAGE - 66)) |
             (1ULL << (PrestoSqlParser::LAST - 66)) |
             (1ULL << (PrestoSqlParser::LATERAL - 66)) |
             (1ULL << (PrestoSqlParser::LEVEL - 66)) |
             (1ULL << (PrestoSqlParser::LIMIT - 66)) |
             (1ULL << (PrestoSqlParser::LOGICAL - 66)) |
             (1ULL << (PrestoSqlParser::MAP - 66)) |
             (1ULL << (PrestoSqlParser::MATERIALIZED - 66)) |
             (1ULL << (PrestoSqlParser::MINUTE - 66)) |
             (1ULL << (PrestoSqlParser::MONTH - 66)) |
             (1ULL << (PrestoSqlParser::NAME - 66)) |
             (1ULL << (PrestoSqlParser::NFC - 66)) |
             (1ULL << (PrestoSqlParser::NFD - 66)) |
             (1ULL << (PrestoSqlParser::NFKC - 66)) |
             (1ULL << (PrestoSqlParser::NFKD - 66)) |
             (1ULL << (PrestoSqlParser::NO - 66)))) != 0) ||
          ((((_la - 130) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 130)) &
            ((1ULL << (PrestoSqlParser::NONE - 130)) |
             (1ULL << (PrestoSqlParser::NULLIF - 130)) |
             (1ULL << (PrestoSqlParser::NULLS - 130)) |
             (1ULL << (PrestoSqlParser::OF - 130)) |
             (1ULL << (PrestoSqlParser::OFFSET - 130)) |
             (1ULL << (PrestoSqlParser::ONLY - 130)) |
             (1ULL << (PrestoSqlParser::OPTION - 130)) |
             (1ULL << (PrestoSqlParser::ORDINALITY - 130)) |
             (1ULL << (PrestoSqlParser::OUTPUT - 130)) |
             (1ULL << (PrestoSqlParser::OVER - 130)) |
             (1ULL << (PrestoSqlParser::PARTITION - 130)) |
             (1ULL << (PrestoSqlParser::PARTITIONS - 130)) |
             (1ULL << (PrestoSqlParser::POSITION - 130)) |
             (1ULL << (PrestoSqlParser::PRECEDING - 130)) |
             (1ULL << (PrestoSqlParser::PRIMARY - 130)) |
             (1ULL << (PrestoSqlParser::PRIVILEGES - 130)) |
             (1ULL << (PrestoSqlParser::PROPERTIES - 130)) |
             (1ULL << (PrestoSqlParser::RANGE - 130)) |
             (1ULL << (PrestoSqlParser::READ - 130)) |
             (1ULL << (PrestoSqlParser::REFRESH - 130)) |
             (1ULL << (PrestoSqlParser::RELY - 130)) |
             (1ULL << (PrestoSqlParser::RENAME - 130)) |
             (1ULL << (PrestoSqlParser::REPEATABLE - 130)) |
             (1ULL << (PrestoSqlParser::REPLACE - 130)) |
             (1ULL << (PrestoSqlParser::RESET - 130)) |
             (1ULL << (PrestoSqlParser::RESPECT - 130)) |
             (1ULL << (PrestoSqlParser::RESTRICT - 130)) |
             (1ULL << (PrestoSqlParser::RETURN - 130)) |
             (1ULL << (PrestoSqlParser::RETURNS - 130)) |
             (1ULL << (PrestoSqlParser::REVOKE - 130)) |
             (1ULL << (PrestoSqlParser::ROLE - 130)) |
             (1ULL << (PrestoSqlParser::ROLES - 130)) |
             (1ULL << (PrestoSqlParser::ROLLBACK - 130)) |
             (1ULL << (PrestoSqlParser::ROW - 130)) |
             (1ULL << (PrestoSqlParser::ROWS - 130)) |
             (1ULL << (PrestoSqlParser::SCHEMA - 130)) |
             (1ULL << (PrestoSqlParser::SCHEMAS - 130)) |
             (1ULL << (PrestoSqlParser::SECOND - 130)) |
             (1ULL << (PrestoSqlParser::SECURITY - 130)) |
             (1ULL << (PrestoSqlParser::SERIALIZABLE - 130)) |
             (1ULL << (PrestoSqlParser::SESSION - 130)) |
             (1ULL << (PrestoSqlParser::SET - 130)) |
             (1ULL << (PrestoSqlParser::SETS - 130)) |
             (1ULL << (PrestoSqlParser::SHOW - 130)) |
             (1ULL << (PrestoSqlParser::SOME - 130)) |
             (1ULL << (PrestoSqlParser::SQL - 130)) |
             (1ULL << (PrestoSqlParser::START - 130)) |
             (1ULL << (PrestoSqlParser::STATS - 130)) |
             (1ULL << (PrestoSqlParser::SUBSTRING - 130)) |
             (1ULL << (PrestoSqlParser::SYSTEM - 130)) |
             (1ULL << (PrestoSqlParser::SYSTEM_TIME - 130)) |
             (1ULL << (PrestoSqlParser::SYSTEM_VERSION - 130)))) != 0) ||
          ((((_la - 195) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 195)) &
            ((1ULL << (PrestoSqlParser::TABLES - 195)) |
             (1ULL << (PrestoSqlParser::TABLESAMPLE - 195)) |
             (1ULL << (PrestoSqlParser::TEMPORARY - 195)) |
             (1ULL << (PrestoSqlParser::TEXT - 195)) |
             (1ULL << (PrestoSqlParser::TIME - 195)) |
             (1ULL << (PrestoSqlParser::TIMESTAMP - 195)) |
             (1ULL << (PrestoSqlParser::TO - 195)) |
             (1ULL << (PrestoSqlParser::TRANSACTION - 195)) |
             (1ULL << (PrestoSqlParser::TRUNCATE - 195)) |
             (1ULL << (PrestoSqlParser::TRY_CAST - 195)) |
             (1ULL << (PrestoSqlParser::TYPE - 195)) |
             (1ULL << (PrestoSqlParser::UNBOUNDED - 195)) |
             (1ULL << (PrestoSqlParser::UNCOMMITTED - 195)) |
             (1ULL << (PrestoSqlParser::UNIQUE - 195)) |
             (1ULL << (PrestoSqlParser::UPDATE - 195)) |
             (1ULL << (PrestoSqlParser::USE - 195)) |
             (1ULL << (PrestoSqlParser::USER - 195)) |
             (1ULL << (PrestoSqlParser::VALIDATE - 195)) |
             (1ULL << (PrestoSqlParser::VERBOSE - 195)) |
             (1ULL << (PrestoSqlParser::VERSION - 195)) |
             (1ULL << (PrestoSqlParser::VIEW - 195)) |
             (1ULL << (PrestoSqlParser::WORK - 195)) |
             (1ULL << (PrestoSqlParser::WRITE - 195)) |
             (1ULL << (PrestoSqlParser::YEAR - 195)) |
             (1ULL << (PrestoSqlParser::ZONE - 195)))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

bool PrestoSqlParser::sempred(
    RuleContext* context,
    size_t ruleIndex,
    size_t predicateIndex) {
  switch (ruleIndex) {
    case 24:
      return queryTermSempred(
          antlrcpp::downCast<QueryTermContext*>(context), predicateIndex);
    case 34:
      return relationSempred(
          antlrcpp::downCast<RelationContext*>(context), predicateIndex);
    case 43:
      return booleanExpressionSempred(
          antlrcpp::downCast<BooleanExpressionContext*>(context),
          predicateIndex);
    case 45:
      return valueExpressionSempred(
          antlrcpp::downCast<ValueExpressionContext*>(context), predicateIndex);
    case 46:
      return primaryExpressionSempred(
          antlrcpp::downCast<PrimaryExpressionContext*>(context),
          predicateIndex);
    case 57:
      return typeSempred(
          antlrcpp::downCast<TypeContext*>(context), predicateIndex);

    default:
      break;
  }
  return true;
}

bool PrestoSqlParser::queryTermSempred(
    QueryTermContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 0:
      return precpred(_ctx, 2);
    case 1:
      return precpred(_ctx, 1);

    default:
      break;
  }
  return true;
}

bool PrestoSqlParser::relationSempred(
    RelationContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 2:
      return precpred(_ctx, 2);

    default:
      break;
  }
  return true;
}

bool PrestoSqlParser::booleanExpressionSempred(
    BooleanExpressionContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 3:
      return precpred(_ctx, 2);
    case 4:
      return precpred(_ctx, 1);

    default:
      break;
  }
  return true;
}

bool PrestoSqlParser::valueExpressionSempred(
    ValueExpressionContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 5:
      return precpred(_ctx, 3);
    case 6:
      return precpred(_ctx, 2);
    case 7:
      return precpred(_ctx, 1);
    case 8:
      return precpred(_ctx, 5);

    default:
      break;
  }
  return true;
}

bool PrestoSqlParser::primaryExpressionSempred(
    PrimaryExpressionContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 9:
      return precpred(_ctx, 14);
    case 10:
      return precpred(_ctx, 12);

    default:
      break;
  }
  return true;
}

bool PrestoSqlParser::typeSempred(
    TypeContext* _localctx,
    size_t predicateIndex) {
  switch (predicateIndex) {
    case 11:
      return precpred(_ctx, 6);

    default:
      break;
  }
  return true;
}

// Static vars and initialization.
std::vector<dfa::DFA> PrestoSqlParser::_decisionToDFA;
atn::PredictionContextCache PrestoSqlParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN PrestoSqlParser::_atn;
std::vector<uint16_t> PrestoSqlParser::_serializedATN;

std::vector<std::string> PrestoSqlParser::_ruleNames = {
    "singleStatement",
    "standaloneExpression",
    "standaloneRoutineBody",
    "statement",
    "query",
    "with",
    "tableElement",
    "columnDefinition",
    "likeClause",
    "properties",
    "property",
    "sqlParameterDeclaration",
    "routineCharacteristics",
    "routineCharacteristic",
    "alterRoutineCharacteristics",
    "alterRoutineCharacteristic",
    "routineBody",
    "returnStatement",
    "externalBodyReference",
    "language",
    "determinism",
    "nullCallClause",
    "externalRoutineName",
    "queryNoWith",
    "queryTerm",
    "queryPrimary",
    "sortItem",
    "querySpecification",
    "groupBy",
    "groupingElement",
    "groupingSet",
    "namedQuery",
    "setQuantifier",
    "selectItem",
    "relation",
    "joinType",
    "joinCriteria",
    "sampledRelation",
    "sampleType",
    "aliasedRelation",
    "columnAliases",
    "relationPrimary",
    "expression",
    "booleanExpression",
    "predicate",
    "valueExpression",
    "primaryExpression",
    "string",
    "nullTreatment",
    "timeZoneSpecifier",
    "comparisonOperator",
    "comparisonQuantifier",
    "booleanValue",
    "interval",
    "intervalField",
    "normalForm",
    "types",
    "type",
    "typeParameter",
    "baseType",
    "whenClause",
    "filter",
    "over",
    "windowFrame",
    "frameBound",
    "updateAssignment",
    "explainOption",
    "transactionMode",
    "levelOfIsolation",
    "callArgument",
    "privilege",
    "qualifiedName",
    "tableVersionExpression",
    "tableVersionState",
    "grantor",
    "principal",
    "roles",
    "identifier",
    "number",
    "constraintSpecification",
    "namedConstraintSpecification",
    "unnamedConstraintSpecification",
    "constraintType",
    "constraintQualifiers",
    "constraintQualifier",
    "constraintRely",
    "constraintEnabled",
    "constraintEnforced",
    "nonReserved"};

std::vector<std::string> PrestoSqlParser::_literalNames = {
    "",
    "'.'",
    "'('",
    "')'",
    "','",
    "'\u003F'",
    "'->'",
    "'['",
    "']'",
    "'=>'",
    "",
    "'ADD'",
    "'ADMIN'",
    "'ALL'",
    "'ALTER'",
    "'ANALYZE'",
    "'AND'",
    "'ANY'",
    "'ARRAY'",
    "'AS'",
    "'ASC'",
    "'AT'",
    "'BEFORE'",
    "'BERNOULLI'",
    "'BETWEEN'",
    "'BY'",
    "'CALL'",
    "'CALLED'",
    "'CASCADE'",
    "'CASE'",
    "'CAST'",
    "'CATALOGS'",
    "'COLUMN'",
    "'COLUMNS'",
    "'COMMENT'",
    "'COMMIT'",
    "'COMMITTED'",
    "'CONSTRAINT'",
    "'CREATE'",
    "'CROSS'",
    "'CUBE'",
    "'CURRENT'",
    "'CURRENT_DATE'",
    "'CURRENT_ROLE'",
    "'CURRENT_TIME'",
    "'CURRENT_TIMESTAMP'",
    "'CURRENT_USER'",
    "'DATA'",
    "'DATE'",
    "'DAY'",
    "'DEALLOCATE'",
    "'DEFINER'",
    "'DELETE'",
    "'DESC'",
    "'DESCRIBE'",
    "'DETERMINISTIC'",
    "'DISABLED'",
    "'DISTINCT'",
    "'DISTRIBUTED'",
    "'DROP'",
    "'ELSE'",
    "'ENABLED'",
    "'END'",
    "'ENFORCED'",
    "'ESCAPE'",
    "'EXCEPT'",
    "'EXCLUDING'",
    "'EXECUTE'",
    "'EXISTS'",
    "'EXPLAIN'",
    "'EXTRACT'",
    "'EXTERNAL'",
    "'FALSE'",
    "'FETCH'",
    "'FILTER'",
    "'FIRST'",
    "'FOLLOWING'",
    "'FOR'",
    "'FORMAT'",
    "'FROM'",
    "'FULL'",
    "'FUNCTION'",
    "'FUNCTIONS'",
    "'GRANT'",
    "'GRANTED'",
    "'GRANTS'",
    "'GRAPHVIZ'",
    "'GROUP'",
    "'GROUPING'",
    "'GROUPS'",
    "'HAVING'",
    "'HOUR'",
    "'IF'",
    "'IGNORE'",
    "'IN'",
    "'INCLUDING'",
    "'INNER'",
    "'INPUT'",
    "'INSERT'",
    "'INTERSECT'",
    "'INTERVAL'",
    "'INTO'",
    "'INVOKER'",
    "'IO'",
    "'IS'",
    "'ISOLATION'",
    "'JSON'",
    "'JOIN'",
    "'KEY'",
    "'LANGUAGE'",
    "'LAST'",
    "'LATERAL'",
    "'LEFT'",
    "'LEVEL'",
    "'LIKE'",
    "'LIMIT'",
    "'LOCALTIME'",
    "'LOCALTIMESTAMP'",
    "'LOGICAL'",
    "'MAP'",
    "'MATERIALIZED'",
    "'MINUTE'",
    "'MONTH'",
    "'NAME'",
    "'NATURAL'",
    "'NFC'",
    "'NFD'",
    "'NFKC'",
    "'NFKD'",
    "'NO'",
    "'NONE'",
    "'NORMALIZE'",
    "'NOT'",
    "'NULL_LITERAL'",
    "'NULLIF'",
    "'NULLS'",
    "'OF'",
    "'OFFSET'",
    "'ON'",
    "'ONLY'",
    "'OPTION'",
    "'OR'",
    "'ORDER'",
    "'ORDINALITY'",
    "'OUTER'",
    "'OUTPUT'",
    "'OVER'",
    "'PARTITION'",
    "'PARTITIONS'",
    "'POSITION'",
    "'PRECEDING'",
    "'PREPARE'",
    "'PRIMARY'",
    "'PRIVILEGES'",
    "'PROPERTIES'",
    "'RANGE'",
    "'READ'",
    "'RECURSIVE'",
    "'REFRESH'",
    "'RELY'",
    "'RENAME'",
    "'REPEATABLE'",
    "'REPLACE'",
    "'RESET'",
    "'RESPECT'",
    "'RESTRICT'",
    "'RETURN'",
    "'RETURNS'",
    "'REVOKE'",
    "'RIGHT'",
    "'ROLE'",
    "'ROLES'",
    "'ROLLBACK'",
    "'ROLLUP'",
    "'ROW'",
    "'ROWS'",
    "'SCHEMA'",
    "'SCHEMAS'",
    "'SECOND'",
    "'SECURITY'",
    "'SELECT'",
    "'SERIALIZABLE'",
    "'SESSION'",
    "'SET'",
    "'SETS'",
    "'SHOW'",
    "'SOME'",
    "'SQL'",
    "'START'",
    "'STATS'",
    "'SUBSTRING'",
    "'SYSTEM'",
    "'SYSTEM_TIME'",
    "'SYSTEM_VERSION'",
    "'TABLE'",
    "'TABLES'",
    "'TABLESAMPLE'",
    "'TEMPORARY'",
    "'TEXT'",
    "'THEN'",
    "'TIME'",
    "'TIMESTAMP'",
    "'TO'",
    "'TRANSACTION'",
    "'TRUE'",
    "'TRUNCATE'",
    "'TRY_CAST'",
    "'TYPE'",
    "'UESCAPE'",
    "'UNBOUNDED'",
    "'UNCOMMITTED'",
    "'UNION'",
    "'UNIQUE'",
    "'UNNEST'",
    "'UPDATE'",
    "'USE'",
    "'USER'",
    "'USING'",
    "'VALIDATE'",
    "'VALUES'",
    "'VERBOSE'",
    "'VERSION'",
    "'VIEW'",
    "'WHEN'",
    "'WHERE'",
    "'WITH'",
    "'WORK'",
    "'WRITE'",
    "'YEAR'",
    "'ZONE'",
    "'='",
    "",
    "'<'",
    "'<='",
    "'>'",
    "'>='",
    "'+'",
    "'-'",
    "'*'",
    "'/'",
    "'%'",
    "'||'"};

std::vector<std::string> PrestoSqlParser::_symbolicNames = {
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "ENGLISH_TOKEN",
    "ADD",
    "ADMIN",
    "ALL",
    "ALTER",
    "ANALYZE",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "AT",
    "BEFORE",
    "BERNOULLI",
    "BETWEEN",
    "BY",
    "CALL",
    "CALLED",
    "CASCADE",
    "CASE",
    "CAST",
    "CATALOGS",
    "COLUMN",
    "COLUMNS",
    "COMMENT",
    "COMMIT",
    "COMMITTED",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "CURRENT_DATE",
    "CURRENT_ROLE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "DATA",
    "DATE",
    "DAY",
    "DEALLOCATE",
    "DEFINER",
    "DELETE",
    "DESC",
    "DESCRIBE",
    "DETERMINISTIC",
    "DISABLED",
    "DISTINCT",
    "DISTRIBUTED",
    "DROP",
    "ELSE",
    "ENABLED",
    "END",
    "ENFORCED",
    "ESCAPE",
    "EXCEPT",
    "EXCLUDING",
    "EXECUTE",
    "EXISTS",
    "EXPLAIN",
    "EXTRACT",
    "EXTERNAL",
    "FALSE",
    "FETCH",
    "FILTER",
    "FIRST",
    "FOLLOWING",
    "FOR",
    "FORMAT",
    "FROM",
    "FULL",
    "FUNCTION",
    "FUNCTIONS",
    "GRANT",
    "GRANTED",
    "GRANTS",
    "GRAPHVIZ",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HAVING",
    "HOUR",
    "IF",
    "IGNORE",
    "IN",
    "INCLUDING",
    "INNER",
    "INPUT",
    "INSERT",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "INVOKER",
    "IO",
    "IS",
    "ISOLATION",
    "JSON",
    "JOIN",
    "KEY",
    "LANGUAGE",
    "LAST",
    "LATERAL",
    "LEFT",
    "LEVEL",
    "LIKE",
    "LIMIT",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOGICAL",
    "MAP",
    "MATERIALIZED",
    "MINUTE",
    "MONTH",
    "NAME",
    "NATURAL",
    "NFC",
    "NFD",
    "NFKC",
    "NFKD",
    "NO",
    "NONE",
    "NORMALIZE",
    "NOT",
    "NULL_LITERAL",
    "NULLIF",
    "NULLS",
    "OF",
    "OFFSET",
    "ON",
    "ONLY",
    "OPTION",
    "OR",
    "ORDER",
    "ORDINALITY",
    "OUTER",
    "OUTPUT",
    "OVER",
    "PARTITION",
    "PARTITIONS",
    "POSITION",
    "PRECEDING",
    "PREPARE",
    "PRIMARY",
    "PRIVILEGES",
    "PROPERTIES",
    "RANGE",
    "READ",
    "RECURSIVE",
    "REFRESH",
    "RELY",
    "RENAME",
    "REPEATABLE",
    "REPLACE",
    "RESET",
    "RESPECT",
    "RESTRICT",
    "RETURN",
    "RETURNS",
    "REVOKE",
    "RIGHT",
    "ROLE",
    "ROLES",
    "ROLLBACK",
    "ROLLUP",
    "ROW",
    "ROWS",
    "SCHEMA",
    "SCHEMAS",
    "SECOND",
    "SECURITY",
    "SELECT",
    "SERIALIZABLE",
    "SESSION",
    "SET",
    "SETS",
    "SHOW",
    "SOME",
    "SQL",
    "START",
    "STATS",
    "SUBSTRING",
    "SYSTEM",
    "SYSTEM_TIME",
    "SYSTEM_VERSION",
    "TABLE",
    "TABLES",
    "TABLESAMPLE",
    "TEMPORARY",
    "TEXT",
    "THEN",
    "TIME",
    "TIMESTAMP",
    "TO",
    "TRANSACTION",
    "TRUE",
    "TRUNCATE",
    "TRY_CAST",
    "TYPE",
    "UESCAPE",
    "UNBOUNDED",
    "UNCOMMITTED",
    "UNION",
    "UNIQUE",
    "UNNEST",
    "UPDATE",
    "USE",
    "USER",
    "USING",
    "VALIDATE",
    "VALUES",
    "VERBOSE",
    "VERSION",
    "VIEW",
    "WHEN",
    "WHERE",
    "WITH",
    "WORK",
    "WRITE",
    "YEAR",
    "ZONE",
    "EQ",
    "NEQ",
    "LT",
    "LTE",
    "GT",
    "GTE",
    "PLUS",
    "MINUS",
    "ASTERISK",
    "SLASH",
    "PERCENT",
    "CONCAT",
    "STRING",
    "UNICODE_STRING",
    "BINARY_LITERAL",
    "INTEGER_VALUE",
    "DECIMAL_VALUE",
    "DOUBLE_VALUE",
    "IDENTIFIER",
    "DIGIT_IDENTIFIER",
    "QUOTED_IDENTIFIER",
    "BACKQUOTED_IDENTIFIER",
    "TIME_WITH_TIME_ZONE",
    "TIMESTAMP_WITH_TIME_ZONE",
    "DOUBLE_PRECISION",
    "SIMPLE_COMMENT",
    "BRACKETED_COMMENT",
    "WS",
    "UNRECOGNIZED",
    "DELIMITER"};

dfa::Vocabulary PrestoSqlParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> PrestoSqlParser::_tokenNames;

PrestoSqlParser::Initializer::Initializer() {
  for (size_t i = 0; i < _symbolicNames.size(); ++i) {
    std::string name = _vocabulary.getLiteralName(i);
    if (name.empty()) {
      name = _vocabulary.getSymbolicName(i);
    }

    if (name.empty()) {
      _tokenNames.push_back("<INVALID>");
    } else {
      _tokenNames.push_back(name);
    }
  }

  static const uint16_t serializedATNSegment0[] = {
      0x3,   0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964,
      0x3,   0x105,  0x812,  0x4,    0x2,    0x9,    0x2,    0x4,    0x3,
      0x9,   0x3,    0x4,    0x4,    0x9,    0x4,    0x4,    0x5,    0x9,
      0x5,   0x4,    0x6,    0x9,    0x6,    0x4,    0x7,    0x9,    0x7,
      0x4,   0x8,    0x9,    0x8,    0x4,    0x9,    0x9,    0x9,    0x4,
      0xa,   0x9,    0xa,    0x4,    0xb,    0x9,    0xb,    0x4,    0xc,
      0x9,   0xc,    0x4,    0xd,    0x9,    0xd,    0x4,    0xe,    0x9,
      0xe,   0x4,    0xf,    0x9,    0xf,    0x4,    0x10,   0x9,    0x10,
      0x4,   0x11,   0x9,    0x11,   0x4,    0x12,   0x9,    0x12,   0x4,
      0x13,  0x9,    0x13,   0x4,    0x14,   0x9,    0x14,   0x4,    0x15,
      0x9,   0x15,   0x4,    0x16,   0x9,    0x16,   0x4,    0x17,   0x9,
      0x17,  0x4,    0x18,   0x9,    0x18,   0x4,    0x19,   0x9,    0x19,
      0x4,   0x1a,   0x9,    0x1a,   0x4,    0x1b,   0x9,    0x1b,   0x4,
      0x1c,  0x9,    0x1c,   0x4,    0x1d,   0x9,    0x1d,   0x4,    0x1e,
      0x9,   0x1e,   0x4,    0x1f,   0x9,    0x1f,   0x4,    0x20,   0x9,
      0x20,  0x4,    0x21,   0x9,    0x21,   0x4,    0x22,   0x9,    0x22,
      0x4,   0x23,   0x9,    0x23,   0x4,    0x24,   0x9,    0x24,   0x4,
      0x25,  0x9,    0x25,   0x4,    0x26,   0x9,    0x26,   0x4,    0x27,
      0x9,   0x27,   0x4,    0x28,   0x9,    0x28,   0x4,    0x29,   0x9,
      0x29,  0x4,    0x2a,   0x9,    0x2a,   0x4,    0x2b,   0x9,    0x2b,
      0x4,   0x2c,   0x9,    0x2c,   0x4,    0x2d,   0x9,    0x2d,   0x4,
      0x2e,  0x9,    0x2e,   0x4,    0x2f,   0x9,    0x2f,   0x4,    0x30,
      0x9,   0x30,   0x4,    0x31,   0x9,    0x31,   0x4,    0x32,   0x9,
      0x32,  0x4,    0x33,   0x9,    0x33,   0x4,    0x34,   0x9,    0x34,
      0x4,   0x35,   0x9,    0x35,   0x4,    0x36,   0x9,    0x36,   0x4,
      0x37,  0x9,    0x37,   0x4,    0x38,   0x9,    0x38,   0x4,    0x39,
      0x9,   0x39,   0x4,    0x3a,   0x9,    0x3a,   0x4,    0x3b,   0x9,
      0x3b,  0x4,    0x3c,   0x9,    0x3c,   0x4,    0x3d,   0x9,    0x3d,
      0x4,   0x3e,   0x9,    0x3e,   0x4,    0x3f,   0x9,    0x3f,   0x4,
      0x40,  0x9,    0x40,   0x4,    0x41,   0x9,    0x41,   0x4,    0x42,
      0x9,   0x42,   0x4,    0x43,   0x9,    0x43,   0x4,    0x44,   0x9,
      0x44,  0x4,    0x45,   0x9,    0x45,   0x4,    0x46,   0x9,    0x46,
      0x4,   0x47,   0x9,    0x47,   0x4,    0x48,   0x9,    0x48,   0x4,
      0x49,  0x9,    0x49,   0x4,    0x4a,   0x9,    0x4a,   0x4,    0x4b,
      0x9,   0x4b,   0x4,    0x4c,   0x9,    0x4c,   0x4,    0x4d,   0x9,
      0x4d,  0x4,    0x4e,   0x9,    0x4e,   0x4,    0x4f,   0x9,    0x4f,
      0x4,   0x50,   0x9,    0x50,   0x4,    0x51,   0x9,    0x51,   0x4,
      0x52,  0x9,    0x52,   0x4,    0x53,   0x9,    0x53,   0x4,    0x54,
      0x9,   0x54,   0x4,    0x55,   0x9,    0x55,   0x4,    0x56,   0x9,
      0x56,  0x4,    0x57,   0x9,    0x57,   0x4,    0x58,   0x9,    0x58,
      0x4,   0x59,   0x9,    0x59,   0x4,    0x5a,   0x9,    0x5a,   0x3,
      0x2,   0x3,    0x2,    0x3,    0x2,    0x3,    0x3,    0x3,    0x3,
      0x3,   0x3,    0x3,    0x4,    0x3,    0x4,    0x3,    0x4,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0xcb,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x5,   0x5,    0xd0,   0xa,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x5,    0x5,    0xd6,   0xa,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x5,    0x5,    0xda,   0xa,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0xe8,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0xec,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0xf0,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0xf4,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0xfc,   0xa,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x100,  0xa,
      0x5,   0x3,    0x5,    0x5,    0x5,    0x103,  0xa,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x5,   0x5,    0x10a,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x7,    0x5,    0x111,
      0xa,   0x5,    0xc,    0x5,    0xe,    0x5,    0x114,  0xb,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x119,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x11d,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x5,    0x5,    0x123,  0xa,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0x12a, 0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x133,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x5,   0x5,    0x13c,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x147,  0xa,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x14e,  0xa,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x158,  0xa,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x15f,  0xa,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x5,    0x5,    0x167,  0xa,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x5,   0x5,    0x16f,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x177,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x181,  0xa,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x188,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0x190, 0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x5,   0x5,    0x195,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x1a0,  0xa,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0x1a5, 0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x5,    0x5,    0x1b0,  0xa,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x1bb,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x7,    0x5,    0x1c5,  0xa,    0x5,    0xc,    0x5,
      0xe,   0x5,    0x1c8,  0xb,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x1cd,  0xa,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x5,    0x5,    0x1d2,  0xa,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x1d8,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x5,   0x5,    0x1e1,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x1ec,  0xa,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x1f5,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x1fa,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x1fe,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0x206, 0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x5,    0x5,    0x20d,  0xa,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x5,    0x5,    0x21a,  0xa,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x21d,  0xa,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x7,    0x5,    0x225,  0xa,    0x5,    0xc,    0x5,    0xe,
      0x5,   0x228,  0xb,    0x5,    0x5,    0x5,    0x22a,  0xa,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x5,    0x5,    0x231,  0xa,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x23a,  0xa,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x240,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x245,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x249,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x7,    0x5,
      0x251, 0xa,    0x5,    0xc,    0x5,    0xe,    0x5,    0x254,  0xb,
      0x5,   0x5,    0x5,    0x256,  0xa,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x5,    0x5,    0x260,  0xa,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x7,   0x5,    0x26b,  0xa,    0x5,    0xc,    0x5,    0xe,    0x5,
      0x26e, 0xb,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x5,   0x5,    0x273,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x278,  0xa,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x27e,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x7,    0x5,    0x285,  0xa,    0x5,    0xc,
      0x5,   0xe,    0x5,    0x288,  0xb,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x5,    0x5,    0x28d,  0xa,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x5,   0x5,    0x294,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x7,    0x5,    0x29a,  0xa,    0x5,
      0xc,   0x5,    0xe,    0x5,    0x29d,  0xb,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x2a1,  0xa,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x2a5,  0xa,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x5,    0x5,    0x2ad,  0xa,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x2b3,  0xa,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x7,    0x5,
      0x2b8, 0xa,    0x5,    0xc,    0x5,    0xe,    0x5,    0x2bb,  0xb,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x2bf,  0xa,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x2c3,  0xa,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x2cd,  0xa,    0x5,    0x3,    0x5,    0x5,    0x5,    0x2d0,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x2d4,
      0xa,   0x5,    0x3,    0x5,    0x5,    0x5,    0x2d7,  0xa,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x7,
      0x5,   0x2dd,  0xa,    0x5,    0xc,    0x5,    0xe,    0x5,    0x2e0,
      0xb,   0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,    0x2e4,
      0xa,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x5,    0x5,    0x2f9,  0xa,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,
      0x5,   0x2ff,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x5,    0x5,    0x305,  0xa,    0x5,    0x5,
      0x5,   0x307,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x5,    0x5,    0x30d,  0xa,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0x313, 0xa,    0x5,    0x5,    0x5,    0x315,  0xa,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x31d,  0xa,    0x5,    0x5,    0x5,
      0x31f, 0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x5,    0x5,    0x332,  0xa,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x5,    0x5,    0x337,  0xa,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x5,   0x5,    0x33e,  0xa,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0x34a, 0xa,    0x5,    0x5,    0x5,    0x34c,  0xa,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x5,    0x5,    0x354,  0xa,    0x5,    0x5,    0x5,
      0x356, 0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x7,    0x5,    0x366,  0xa,    0x5,
      0xc,   0x5,    0xe,    0x5,    0x369,  0xb,    0x5,    0x5,    0x5,
      0x36b, 0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0x36f, 0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0x373, 0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x7,    0x5,    0x383,  0xa,    0x5,
      0xc,   0x5,    0xe,    0x5,    0x386,  0xb,    0x5,    0x5,    0x5,
      0x388, 0xa,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,   0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,
      0x7,   0x5,    0x396,  0xa,    0x5,    0xc,    0x5,    0xe,    0x5,
      0x399, 0xb,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0x39d, 0xa,    0x5,    0x5,    0x5,    0x39f,  0xa,    0x5,    0x3,
      0x6,   0x5,    0x6,    0x3a2,  0xa,    0x6,    0x3,    0x6,    0x3,
      0x6,   0x3,    0x7,    0x3,    0x7,    0x5,    0x7,    0x3a8,  0xa,
      0x7,   0x3,    0x7,    0x3,    0x7,    0x3,    0x7,    0x7,    0x7,
      0x3ad, 0xa,    0x7,    0xc,    0x7,    0xe,    0x7,    0x3b0,  0xb,
      0x7,   0x3,    0x8,    0x3,    0x8,    0x3,    0x8,    0x5,    0x8,
      0x3b5, 0xa,    0x8,    0x3,    0x9,    0x3,    0x9,    0x3,    0x9,
      0x3,   0x9,    0x5,    0x9,    0x3bb,  0xa,    0x9,    0x3,    0x9,
      0x3,   0x9,    0x5,    0x9,    0x3bf,  0xa,    0x9,    0x3,    0x9,
      0x3,   0x9,    0x5,    0x9,    0x3c3,  0xa,    0x9,    0x3,    0xa,
      0x3,   0xa,    0x3,    0xa,    0x3,    0xa,    0x5,    0xa,    0x3c9,
      0xa,   0xa,    0x3,    0xb,    0x3,    0xb,    0x3,    0xb,    0x3,
      0xb,   0x7,    0xb,    0x3cf,  0xa,    0xb,    0xc,    0xb,    0xe,
      0xb,   0x3d2,  0xb,    0xb,    0x3,    0xb,    0x3,    0xb,    0x3,
      0xc,   0x3,    0xc,    0x3,    0xc,    0x3,    0xc,    0x3,    0xd,
      0x3,   0xd,    0x3,    0xd,    0x3,    0xe,    0x7,    0xe,    0x3de,
      0xa,   0xe,    0xc,    0xe,    0xe,    0xe,    0x3e1,  0xb,    0xe,
      0x3,   0xf,    0x3,    0xf,    0x3,    0xf,    0x3,    0xf,    0x5,
      0xf,   0x3e7,  0xa,    0xf,    0x3,    0x10,   0x7,    0x10,   0x3ea,
      0xa,   0x10,   0xc,    0x10,   0xe,    0x10,   0x3ed,  0xb,    0x10,
      0x3,   0x11,   0x3,    0x11,   0x3,    0x12,   0x3,    0x12,   0x5,
      0x12,  0x3f3,  0xa,    0x12,   0x3,    0x13,   0x3,    0x13,   0x3,
      0x13,  0x3,    0x14,   0x3,    0x14,   0x3,    0x14,   0x5,    0x14,
      0x3fb, 0xa,    0x14,   0x3,    0x15,   0x3,    0x15,   0x5,    0x15,
      0x3ff, 0xa,    0x15,   0x3,    0x16,   0x3,    0x16,   0x3,    0x16,
      0x5,   0x16,   0x404,  0xa,    0x16,   0x3,    0x17,   0x3,    0x17,
      0x3,   0x17,   0x3,    0x17,   0x3,    0x17,   0x3,    0x17,   0x3,
      0x17,  0x3,    0x17,   0x3,    0x17,   0x5,    0x17,   0x40f,  0xa,
      0x17,  0x3,    0x18,   0x3,    0x18,   0x3,    0x19,   0x3,    0x19,
      0x3,   0x19,   0x3,    0x19,   0x3,    0x19,   0x3,    0x19,   0x7,
      0x19,  0x419,  0xa,    0x19,   0xc,    0x19,   0xe,    0x19,   0x41c,
      0xb,   0x19,   0x5,    0x19,   0x41e,  0xa,    0x19,   0x3,    0x19,
      0x3,   0x19,   0x3,    0x19,   0x5,    0x19,   0x423,  0xa,    0x19,
      0x5,   0x19,   0x425,  0xa,    0x19,   0x3,    0x19,   0x3,    0x19,
      0x3,   0x19,   0x3,    0x19,   0x3,    0x19,   0x3,    0x19,   0x3,
      0x19,  0x5,    0x19,   0x42e,  0xa,    0x19,   0x5,    0x19,   0x430,
      0xa,   0x19,   0x3,    0x1a,   0x3,    0x1a,   0x3,    0x1a,   0x3,
      0x1a,  0x3,    0x1a,   0x3,    0x1a,   0x5,    0x1a,   0x438,  0xa,
      0x1a,  0x3,    0x1a,   0x3,    0x1a,   0x3,    0x1a,   0x3,    0x1a,
      0x5,   0x1a,   0x43e,  0xa,    0x1a,   0x3,    0x1a,   0x7,    0x1a,
      0x441, 0xa,    0x1a,   0xc,    0x1a,   0xe,    0x1a,   0x444,  0xb,
      0x1a,  0x3,    0x1b,   0x3,    0x1b,   0x3,    0x1b,   0x3,    0x1b,
      0x3,   0x1b,   0x3,    0x1b,   0x3,    0x1b,   0x7,    0x1b,   0x44d,
      0xa,   0x1b,   0xc,    0x1b,   0xe,    0x1b,   0x450,  0xb,    0x1b,
      0x3,   0x1b,   0x3,    0x1b,   0x3,    0x1b,   0x3,    0x1b,   0x5,
      0x1b,  0x456,  0xa,    0x1b,   0x3,    0x1c,   0x3,    0x1c,   0x5,
      0x1c,  0x45a,  0xa,    0x1c,   0x3,    0x1c,   0x3,    0x1c,   0x5,
      0x1c,  0x45e,  0xa,    0x1c,   0x3,    0x1d,   0x3,    0x1d,   0x5,
      0x1d,  0x462,  0xa,    0x1d,   0x3,    0x1d,   0x3,    0x1d,   0x3,
      0x1d,  0x7,    0x1d,   0x467,  0xa,    0x1d,   0xc,    0x1d,   0xe,
      0x1d,  0x46a,  0xb,    0x1d,   0x3,    0x1d,   0x3,    0x1d,   0x3,
      0x1d,  0x3,    0x1d,   0x7,    0x1d,   0x470,  0xa,    0x1d,   0xc,
      0x1d,  0xe,    0x1d,   0x473,  0xb,    0x1d,   0x5,    0x1d,   0x475,
      0xa,   0x1d,   0x3,    0x1d,   0x3,    0x1d,   0x5,    0x1d,   0x479,
      0xa,   0x1d,   0x3,    0x1d,   0x3,    0x1d,   0x3,    0x1d,   0x5,
      0x1d,  0x47e,  0xa,    0x1d,   0x3,    0x1d,   0x3,    0x1d,   0x5,
      0x1d,  0x482,  0xa,    0x1d,   0x3,    0x1e,   0x5,    0x1e,   0x485,
      0xa,   0x1e,   0x3,    0x1e,   0x3,    0x1e,   0x3,    0x1e,   0x7,
      0x1e,  0x48a,  0xa,    0x1e,   0xc,    0x1e,   0xe,    0x1e,   0x48d,
      0xb,   0x1e,   0x3,    0x1f,   0x3,    0x1f,   0x3,    0x1f,   0x3,
      0x1f,  0x3,    0x1f,   0x3,    0x1f,   0x7,    0x1f,   0x495,  0xa,
      0x1f,  0xc,    0x1f,   0xe,    0x1f,   0x498,  0xb,    0x1f,   0x5,
      0x1f,  0x49a,  0xa,    0x1f,   0x3,    0x1f,   0x3,    0x1f,   0x3,
      0x1f,  0x3,    0x1f,   0x3,    0x1f,   0x3,    0x1f,   0x7,    0x1f,
      0x4a2, 0xa,    0x1f,   0xc,    0x1f,   0xe,    0x1f,   0x4a5,  0xb,
      0x1f,  0x5,    0x1f,   0x4a7,  0xa,    0x1f,   0x3,    0x1f,   0x3,
      0x1f,  0x3,    0x1f,   0x3,    0x1f,   0x3,    0x1f,   0x3,    0x1f,
      0x3,   0x1f,   0x7,    0x1f,   0x4b0,  0xa,    0x1f,   0xc,    0x1f,
      0xe,   0x1f,   0x4b3,  0xb,    0x1f,   0x3,    0x1f,   0x3,    0x1f,
      0x5,   0x1f,   0x4b7,  0xa,    0x1f,   0x3,    0x20,   0x3,    0x20,
      0x3,   0x20,   0x3,    0x20,   0x7,    0x20,   0x4bd,  0xa,    0x20,
      0xc,   0x20,   0xe,    0x20,   0x4c0,  0xb,    0x20,   0x5,    0x20,
      0x4c2, 0xa,    0x20,   0x3,    0x20,   0x3,    0x20,   0x5,    0x20,
      0x4c6, 0xa,    0x20,   0x3,    0x21,   0x3,    0x21,   0x5,    0x21,
      0x4ca, 0xa,    0x21,   0x3,    0x21,   0x3,    0x21,   0x3,    0x21,
      0x3,   0x21,   0x3,    0x21,   0x3,    0x22,   0x3,    0x22,   0x3,
      0x23,  0x3,    0x23,   0x5,    0x23,   0x4d5,  0xa,    0x23,   0x3,
      0x23,  0x5,    0x23,   0x4d8,  0xa,    0x23,   0x3,    0x23,   0x3,
      0x23,  0x3,    0x23,   0x3,    0x23,   0x3,    0x23,   0x5,    0x23,
      0x4df, 0xa,    0x23,   0x3,    0x24,   0x3,    0x24,   0x3,    0x24,
      0x3,   0x24,   0x3,    0x24,   0x3,    0x24,   0x3,    0x24,   0x3,
      0x24,  0x3,    0x24,   0x3,    0x24,   0x3,    0x24,   0x3,    0x24,
      0x3,   0x24,   0x3,    0x24,   0x3,    0x24,   0x3,    0x24,   0x3,
      0x24,  0x5,    0x24,   0x4f2,  0xa,    0x24,   0x7,    0x24,   0x4f4,
      0xa,   0x24,   0xc,    0x24,   0xe,    0x24,   0x4f7,  0xb,    0x24,
      0x3,   0x25,   0x5,    0x25,   0x4fa,  0xa,    0x25,   0x3,    0x25,
      0x3,   0x25,   0x5,    0x25,   0x4fe,  0xa,    0x25,   0x3,    0x25,
      0x3,   0x25,   0x5,    0x25,   0x502,  0xa,    0x25,   0x3,    0x25,
      0x3,   0x25,   0x5,    0x25,   0x506,  0xa,    0x25,   0x5,    0x25,
      0x508, 0xa,    0x25,   0x3,    0x26,   0x3,    0x26,   0x3,    0x26,
      0x3,   0x26,   0x3,    0x26,   0x3,    0x26,   0x3,    0x26,   0x7,
      0x26,  0x511,  0xa,    0x26,   0xc,    0x26,   0xe,    0x26,   0x514,
      0xb,   0x26,   0x3,    0x26,   0x3,    0x26,   0x5,    0x26,   0x518,
      0xa,   0x26,   0x3,    0x27,   0x3,    0x27,   0x3,    0x27,   0x3,
      0x27,  0x3,    0x27,   0x3,    0x27,   0x3,    0x27,   0x5,    0x27,
      0x521, 0xa,    0x27,   0x3,    0x28,   0x3,    0x28,   0x3,    0x29,
      0x3,   0x29,   0x5,    0x29,   0x527,  0xa,    0x29,   0x3,    0x29,
      0x3,   0x29,   0x5,    0x29,   0x52b,  0xa,    0x29,   0x5,    0x29,
      0x52d, 0xa,    0x29,   0x3,    0x2a,   0x3,    0x2a,   0x3,    0x2a,
      0x3,   0x2a,   0x7,    0x2a,   0x533,  0xa,    0x2a,   0xc,    0x2a,
      0xe,   0x2a,   0x536,  0xb,    0x2a,   0x3,    0x2a,   0x3,    0x2a,
      0x3,   0x2b,   0x3,    0x2b,   0x5,    0x2b,   0x53c,  0xa,    0x2b,
      0x3,   0x2b,   0x3,    0x2b,   0x3,    0x2b,   0x3,    0x2b,   0x3,
      0x2b,  0x3,    0x2b,   0x3,    0x2b,   0x3,    0x2b,   0x3,    0x2b,
      0x7,   0x2b,   0x547,  0xa,    0x2b,   0xc,    0x2b,   0xe,    0x2b,
      0x54a, 0xb,    0x2b,   0x3,    0x2b,   0x3,    0x2b,   0x3,    0x2b,
      0x5,   0x2b,   0x54f,  0xa,    0x2b,   0x3,    0x2b,   0x3,    0x2b,
      0x3,   0x2b,   0x3,    0x2b,   0x3,    0x2b,   0x3,    0x2b,   0x3,
      0x2b,  0x3,    0x2b,   0x3,    0x2b,   0x5,    0x2b,   0x55a,  0xa,
      0x2b,  0x3,    0x2c,   0x3,    0x2c,   0x3,    0x2d,   0x3,    0x2d,
      0x3,   0x2d,   0x5,    0x2d,   0x561,  0xa,    0x2d,   0x3,    0x2d,
      0x3,   0x2d,   0x5,    0x2d,   0x565,  0xa,    0x2d,   0x3,    0x2d,
      0x3,   0x2d,   0x3,    0x2d,   0x3,    0x2d,   0x3,    0x2d,   0x3,
      0x2d,  0x7,    0x2d,   0x56d,  0xa,    0x2d,   0xc,    0x2d,   0xe,
      0x2d,  0x570,  0xb,    0x2d,   0x3,    0x2e,   0x3,    0x2e,   0x3,
      0x2e,  0x3,    0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x3,    0x2e,
      0x3,   0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x5,    0x2e,   0x57c,
      0xa,   0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x3,
      0x2e,  0x3,    0x2e,   0x3,    0x2e,   0x5,    0x2e,   0x584,  0xa,
      0x2e,  0x3,    0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x3,    0x2e,
      0x3,   0x2e,   0x7,    0x2e,   0x58b,  0xa,    0x2e,   0xc,    0x2e,
      0xe,   0x2e,   0x58e,  0xb,    0x2e,   0x3,    0x2e,   0x3,    0x2e,
      0x3,   0x2e,   0x5,    0x2e,   0x593,  0xa,    0x2e,   0x3,    0x2e,
      0x3,   0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x3,
      0x2e,  0x5,    0x2e,   0x59b,  0xa,    0x2e,   0x3,    0x2e,   0x3,
      0x2e,  0x3,    0x2e,   0x3,    0x2e,   0x5,    0x2e,   0x5a1,  0xa,
      0x2e,  0x3,    0x2e,   0x3,    0x2e,   0x5,    0x2e,   0x5a5,  0xa,
      0x2e,  0x3,    0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x5,    0x2e,
      0x5aa, 0xa,    0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x3,    0x2e,
      0x5,   0x2e,   0x5af,  0xa,    0x2e,   0x3,    0x2f,   0x3,    0x2f,
      0x3,   0x2f,   0x3,    0x2f,   0x5,    0x2f,   0x5b5,  0xa,    0x2f,
      0x3,   0x2f,   0x3,    0x2f,   0x3,    0x2f,   0x3,    0x2f,   0x3,
      0x2f,  0x3,    0x2f,   0x3,    0x2f,   0x3,    0x2f,   0x3,    0x2f,
      0x3,   0x2f,   0x3,    0x2f,   0x3,    0x2f,   0x7,    0x2f,   0x5c3,
      0xa,   0x2f,   0xc,    0x2f,   0xe,    0x2f,   0x5c6,  0xb,    0x2f,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x6,    0x30,   0x5e0,  0xa,    0x30,   0xd,
      0x30,  0xe,    0x30,   0x5e1,  0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x7,   0x30,   0x5eb,  0xa,    0x30,   0xc,    0x30,   0xe,    0x30,
      0x5ee, 0xb,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x5,
      0x30,  0x5f7,  0xa,    0x30,   0x3,    0x30,   0x5,    0x30,   0x5fa,
      0xa,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x5,
      0x30,  0x5ff,  0xa,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x7,    0x30,   0x604,  0xa,    0x30,   0xc,    0x30,   0xe,
      0x30,  0x607,  0xb,    0x30,   0x5,    0x30,   0x609,  0xa,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x7,    0x30,   0x610,  0xa,    0x30,   0xc,    0x30,   0xe,
      0x30,  0x613,  0xb,    0x30,   0x5,    0x30,   0x615,  0xa,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x5,    0x30,   0x619,  0xa,    0x30,
      0x3,   0x30,   0x5,    0x30,   0x61c,  0xa,    0x30,   0x3,    0x30,
      0x5,   0x30,   0x61f,  0xa,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x7,    0x30,   0x629,  0xa,    0x30,   0xc,
      0x30,  0xe,    0x30,   0x62c,  0xb,    0x30,   0x5,    0x30,   0x62e,
      0xa,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x6,    0x30,   0x63f,  0xa,
      0x30,  0xd,    0x30,   0xe,    0x30,   0x640,  0x3,    0x30,   0x3,
      0x30,  0x5,    0x30,   0x645,  0xa,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x6,    0x30,   0x64b,  0xa,
      0x30,  0xd,    0x30,   0xe,    0x30,   0x64c,  0x3,    0x30,   0x3,
      0x30,  0x5,    0x30,   0x651,  0xa,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x7,    0x30,   0x668,  0xa,    0x30,   0xc,
      0x30,  0xe,    0x30,   0x66b,  0xb,    0x30,   0x5,    0x30,   0x66d,
      0xa,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x5,    0x30,
      0x676, 0xa,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x5,    0x30,   0x67c,  0xa,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x5,    0x30,   0x682,
      0xa,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x5,    0x30,   0x688,  0xa,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x5,    0x30,   0x692,  0xa,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x5,    0x30,   0x69b,  0xa,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x7,    0x30,   0x6af,  0xa,    0x30,   0xc,    0x30,   0xe,
      0x30,  0x6b2,  0xb,    0x30,   0x5,    0x30,   0x6b4,  0xa,    0x30,
      0x3,   0x30,   0x5,    0x30,   0x6b7,  0xa,    0x30,   0x3,    0x30,
      0x3,   0x30,   0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x30,   0x7,    0x30,   0x6c1,  0xa,
      0x30,  0xc,    0x30,   0xe,    0x30,   0x6c4,  0xb,    0x30,   0x3,
      0x31,  0x3,    0x31,   0x3,    0x31,   0x3,    0x31,   0x5,    0x31,
      0x6ca, 0xa,    0x31,   0x5,    0x31,   0x6cc,  0xa,    0x31,   0x3,
      0x32,  0x3,    0x32,   0x3,    0x32,   0x3,    0x32,   0x5,    0x32,
      0x6d2, 0xa,    0x32,   0x3,    0x33,   0x3,    0x33,   0x3,    0x33,
      0x3,   0x33,   0x3,    0x33,   0x3,    0x33,   0x5,    0x33,   0x6da,
      0xa,   0x33,   0x3,    0x34,   0x3,    0x34,   0x3,    0x35,   0x3,
      0x35,  0x3,    0x36,   0x3,    0x36,   0x3,    0x37,   0x3,    0x37,
      0x5,   0x37,   0x6e4,  0xa,    0x37,   0x3,    0x37,   0x3,    0x37,
      0x3,   0x37,   0x3,    0x37,   0x5,    0x37,   0x6ea,  0xa,    0x37,
      0x3,   0x38,   0x3,    0x38,   0x3,    0x39,   0x3,    0x39,   0x3,
      0x3a,  0x3,    0x3a,   0x3,    0x3a,   0x3,    0x3a,   0x7,    0x3a,
      0x6f4, 0xa,    0x3a,   0xc,    0x3a,   0xe,    0x3a,   0x6f7,  0xb,
      0x3a,  0x5,    0x3a,   0x6f9,  0xa,    0x3a,   0x3,    0x3a,   0x3,
      0x3a,  0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,
      0x3,   0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x3,
      0x3b,  0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,
      0x3,   0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x3,
      0x3b,  0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x7,    0x3b,
      0x712, 0xa,    0x3b,   0xc,    0x3b,   0xe,    0x3b,   0x715,  0xb,
      0x3b,  0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,
      0x3,   0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x7,    0x3b,   0x71e,
      0xa,   0x3b,   0xc,    0x3b,   0xe,    0x3b,   0x721,  0xb,    0x3b,
      0x3,   0x3b,   0x3,    0x3b,   0x5,    0x3b,   0x725,  0xa,    0x3b,
      0x3,   0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x3,
      0x3b,  0x5,    0x3b,   0x72c,  0xa,    0x3b,   0x3,    0x3b,   0x3,
      0x3b,  0x7,    0x3b,   0x730,  0xa,    0x3b,   0xc,    0x3b,   0xe,
      0x3b,  0x733,  0xb,    0x3b,   0x3,    0x3c,   0x3,    0x3c,   0x5,
      0x3c,  0x737,  0xa,    0x3c,   0x3,    0x3d,   0x3,    0x3d,   0x3,
      0x3d,  0x3,    0x3d,   0x5,    0x3d,   0x73d,  0xa,    0x3d,   0x3,
      0x3e,  0x3,    0x3e,   0x3,    0x3e,   0x3,    0x3e,   0x3,    0x3e,
      0x3,   0x3f,   0x3,    0x3f,   0x3,    0x3f,   0x3,    0x3f,   0x3,
      0x3f,  0x3,    0x3f,   0x3,    0x40,   0x3,    0x40,   0x3,    0x40,
      0x3,   0x40,   0x3,    0x40,   0x3,    0x40,   0x3,    0x40,   0x7,
      0x40,  0x751,  0xa,    0x40,   0xc,    0x40,   0xe,    0x40,   0x754,
      0xb,   0x40,   0x5,    0x40,   0x756,  0xa,    0x40,   0x3,    0x40,
      0x3,   0x40,   0x3,    0x40,   0x3,    0x40,   0x3,    0x40,   0x7,
      0x40,  0x75d,  0xa,    0x40,   0xc,    0x40,   0xe,    0x40,   0x760,
      0xb,   0x40,   0x5,    0x40,   0x762,  0xa,    0x40,   0x3,    0x40,
      0x5,   0x40,   0x765,  0xa,    0x40,   0x3,    0x40,   0x3,    0x40,
      0x3,   0x41,   0x3,    0x41,   0x3,    0x41,   0x3,    0x41,   0x3,
      0x41,  0x3,    0x41,   0x3,    0x41,   0x3,    0x41,   0x3,    0x41,
      0x3,   0x41,   0x3,    0x41,   0x3,    0x41,   0x3,    0x41,   0x3,
      0x41,  0x3,    0x41,   0x3,    0x41,   0x3,    0x41,   0x3,    0x41,
      0x3,   0x41,   0x3,    0x41,   0x3,    0x41,   0x3,    0x41,   0x3,
      0x41,  0x3,    0x41,   0x5,    0x41,   0x781,  0xa,    0x41,   0x3,
      0x42,  0x3,    0x42,   0x3,    0x42,   0x3,    0x42,   0x3,    0x42,
      0x3,   0x42,   0x3,    0x42,   0x3,    0x42,   0x3,    0x42,   0x5,
      0x42,  0x78c,  0xa,    0x42,   0x3,    0x43,   0x3,    0x43,   0x3,
      0x43,  0x3,    0x43,   0x3,    0x44,   0x3,    0x44,   0x3,    0x44,
      0x3,   0x44,   0x5,    0x44,   0x796,  0xa,    0x44,   0x3,    0x45,
      0x3,   0x45,   0x3,    0x45,   0x3,    0x45,   0x3,    0x45,   0x5,
      0x45,  0x79d,  0xa,    0x45,   0x3,    0x46,   0x3,    0x46,   0x3,
      0x46,  0x3,    0x46,   0x3,    0x46,   0x3,    0x46,   0x3,    0x46,
      0x5,   0x46,   0x7a6,  0xa,    0x46,   0x3,    0x47,   0x3,    0x47,
      0x3,   0x47,   0x3,    0x47,   0x3,    0x47,   0x5,    0x47,   0x7ad,
      0xa,   0x47,   0x3,    0x48,   0x3,    0x48,   0x3,    0x48,   0x3,
      0x48,  0x5,    0x48,   0x7b3,  0xa,    0x48,   0x3,    0x49,   0x3,
      0x49,  0x3,    0x49,   0x7,    0x49,   0x7b8,  0xa,    0x49,   0xc,
      0x49,  0xe,    0x49,   0x7bb,  0xb,    0x49,   0x3,    0x4a,   0x3,
      0x4a,  0x3,    0x4a,   0x3,    0x4a,   0x3,    0x4a,   0x3,    0x4b,
      0x3,   0x4b,   0x3,    0x4b,   0x5,    0x4b,   0x7c5,  0xa,    0x4b,
      0x3,   0x4c,   0x3,    0x4c,   0x3,    0x4c,   0x5,    0x4c,   0x7ca,
      0xa,   0x4c,   0x3,    0x4d,   0x3,    0x4d,   0x3,    0x4d,   0x3,
      0x4d,  0x3,    0x4d,   0x5,    0x4d,   0x7d1,  0xa,    0x4d,   0x3,
      0x4e,  0x3,    0x4e,   0x3,    0x4e,   0x7,    0x4e,   0x7d6,  0xa,
      0x4e,  0xc,    0x4e,   0xe,    0x4e,   0x7d9,  0xb,    0x4e,   0x3,
      0x4f,  0x3,    0x4f,   0x3,    0x4f,   0x3,    0x4f,   0x3,    0x4f,
      0x5,   0x4f,   0x7e0,  0xa,    0x4f,   0x3,    0x50,   0x3,    0x50,
      0x3,   0x50,   0x5,    0x50,   0x7e5,  0xa,    0x50,   0x3,    0x51,
      0x3,   0x51,   0x5,    0x51,   0x7e9,  0xa,    0x51,   0x3,    0x52,
      0x3,   0x52,   0x3,    0x52,   0x3,    0x52,   0x3,    0x53,   0x3,
      0x53,  0x3,    0x53,   0x5,    0x53,   0x7f2,  0xa,    0x53,   0x3,
      0x54,  0x3,    0x54,   0x3,    0x54,   0x5,    0x54,   0x7f7,  0xa,
      0x54,  0x3,    0x55,   0x7,    0x55,   0x7fa,  0xa,    0x55,   0xc,
      0x55,  0xe,    0x55,   0x7fd,  0xb,    0x55,   0x3,    0x56,   0x3,
      0x56,  0x3,    0x56,   0x5,    0x56,   0x802,  0xa,    0x56,   0x3,
      0x57,  0x3,    0x57,   0x3,    0x57,   0x5,    0x57,   0x807,  0xa,
      0x57,  0x3,    0x58,   0x3,    0x58,   0x3,    0x59,   0x3,    0x59,
      0x3,   0x59,   0x5,    0x59,   0x80e,  0xa,    0x59,   0x3,    0x5a,
      0x3,   0x5a,   0x3,    0x5a,   0x2,    0x8,    0x32,   0x46,   0x58,
      0x5c,  0x5e,   0x74,   0x5b,   0x2,    0x4,    0x6,    0x8,    0xa,
      0xc,   0xe,    0x10,   0x12,   0x14,   0x16,   0x18,   0x1a,   0x1c,
      0x1e,  0x20,   0x22,   0x24,   0x26,   0x28,   0x2a,   0x2c,   0x2e,
      0x30,  0x32,   0x34,   0x36,   0x38,   0x3a,   0x3c,   0x3e,   0x40,
      0x42,  0x44,   0x46,   0x48,   0x4a,   0x4c,   0x4e,   0x50,   0x52,
      0x54,  0x56,   0x58,   0x5a,   0x5c,   0x5e,   0x60,   0x62,   0x64,
      0x66,  0x68,   0x6a,   0x6c,   0x6e,   0x70,   0x72,   0x74,   0x76,
      0x78,  0x7a,   0x7c,   0x7e,   0x80,   0x82,   0x84,   0x86,   0x88,
      0x8a,  0x8c,   0x8e,   0x90,   0x92,   0x94,   0x96,   0x98,   0x9a,
      0x9c,  0x9e,   0xa0,   0xa2,   0xa4,   0xa6,   0xa8,   0xaa,   0xac,
      0xae,  0xb0,   0xb2,   0x2,    0x1b,   0x4,    0x2,    0x1e,   0x1e,
      0xa7,  0xa7,   0x4,    0x2,    0x35,   0x35,   0x68,   0x68,   0x4,
      0x2,   0x51,   0x51,   0x60,   0x60,   0x4,    0x2,    0x44,   0x44,
      0x61,  0x61,   0x3,    0x2,    0xb0,   0xb1,   0x4,    0x2,    0xf,
      0xf,   0xf7,   0xf7,   0x4,    0x2,    0x43,   0x43,   0xd5,   0xd5,
      0x4,   0x2,    0x16,   0x16,   0x37,   0x37,   0x4,    0x2,    0x4d,
      0x4d,  0x70,   0x70,   0x4,    0x2,    0xf,    0xf,    0x3b,   0x3b,
      0x4,   0x2,    0x19,   0x19,   0xc1,   0xc1,   0x3,    0x2,    0xee,
      0xef,  0x3,    0x2,    0xf0,   0xf2,   0x3,    0x2,    0xe8,   0xed,
      0x5,   0x2,    0xf,    0xf,    0x13,   0x13,   0xbc,   0xbc,   0x4,
      0x2,   0x4a,   0x4a,   0xce,   0xce,   0x7,    0x2,    0x33,   0x33,
      0x5d,  0x5d,   0x7b,   0x7c,   0xb4,   0xb4,   0xe6,   0xe6,   0x3,
      0x2,   0x7f,   0x82,   0x4,    0x2,    0x4e,   0x4e,   0x98,   0x98,
      0x5,   0x2,    0x58,   0x58,   0x6c,   0x6c,   0xc8,   0xc8,   0x6,
      0x2,   0x3c,   0x3c,   0x69,   0x69,   0x78,   0x78,   0xdc,   0xdc,
      0x4,   0x2,    0x8d,   0x8d,   0xe5,   0xe5,   0x5,    0x2,    0xc2,
      0xc3,  0xcb,   0xcb,   0xdf,   0xdf,   0x4,    0x2,    0x3a,   0x3a,
      0x3f,  0x3f,   0x35,   0x2,    0xd,    0xf,    0x11,   0x11,   0x13,
      0x14,  0x16,   0x19,   0x1c,   0x1e,   0x21,   0x26,   0x2b,   0x2b,
      0x2d,  0x2d,   0x31,   0x33,   0x35,   0x35,   0x37,   0x37,   0x39,
      0x3a,  0x3c,   0x3c,   0x3f,   0x3f,   0x41,   0x41,   0x44,   0x44,
      0x47,  0x47,   0x49,   0x49,   0x4b,   0x4e,   0x50,   0x50,   0x53,
      0x58,  0x5b,   0x5b,   0x5d,   0x5f,   0x61,   0x61,   0x63,   0x63,
      0x66,  0x66,   0x68,   0x69,   0x6b,   0x6c,   0x6e,   0x71,   0x73,
      0x73,  0x75,   0x75,   0x78,   0x7d,   0x7f,   0x84,   0x88,   0x8b,
      0x8d,  0x8e,   0x91,   0x91,   0x93,   0x98,   0x9a,   0x9e,   0xa0,
      0xaa,  0xac,   0xae,   0xb0,   0xb5,   0xb7,   0xc3,   0xc5,   0xc8,
      0xca,  0xcd,   0xcf,   0xd1,   0xd3,   0xd4,   0xd6,   0xd6,   0xd8,
      0xda,  0xdc,   0xdc,   0xde,   0xe0,   0xe4,   0xe7,   0x2,    0x94e,
      0x2,   0xb4,   0x3,    0x2,    0x2,    0x2,    0x4,    0xb7,   0x3,
      0x2,   0x2,    0x2,    0x6,    0xba,   0x3,    0x2,    0x2,    0x2,
      0x8,   0x39e,  0x3,    0x2,    0x2,    0x2,    0xa,    0x3a1,  0x3,
      0x2,   0x2,    0x2,    0xc,    0x3a5,  0x3,    0x2,    0x2,    0x2,
      0xe,   0x3b4,  0x3,    0x2,    0x2,    0x2,    0x10,   0x3b6,  0x3,
      0x2,   0x2,    0x2,    0x12,   0x3c4,  0x3,    0x2,    0x2,    0x2,
      0x14,  0x3ca,  0x3,    0x2,    0x2,    0x2,    0x16,   0x3d5,  0x3,
      0x2,   0x2,    0x2,    0x18,   0x3d9,  0x3,    0x2,    0x2,    0x2,
      0x1a,  0x3df,  0x3,    0x2,    0x2,    0x2,    0x1c,   0x3e6,  0x3,
      0x2,   0x2,    0x2,    0x1e,   0x3eb,  0x3,    0x2,    0x2,    0x2,
      0x20,  0x3ee,  0x3,    0x2,    0x2,    0x2,    0x22,   0x3f2,  0x3,
      0x2,   0x2,    0x2,    0x24,   0x3f4,  0x3,    0x2,    0x2,    0x2,
      0x26,  0x3f7,  0x3,    0x2,    0x2,    0x2,    0x28,   0x3fe,  0x3,
      0x2,   0x2,    0x2,    0x2a,   0x403,  0x3,    0x2,    0x2,    0x2,
      0x2c,  0x40e,  0x3,    0x2,    0x2,    0x2,    0x2e,   0x410,  0x3,
      0x2,   0x2,    0x2,    0x30,   0x412,  0x3,    0x2,    0x2,    0x2,
      0x32,  0x431,  0x3,    0x2,    0x2,    0x2,    0x34,   0x455,  0x3,
      0x2,   0x2,    0x2,    0x36,   0x457,  0x3,    0x2,    0x2,    0x2,
      0x38,  0x45f,  0x3,    0x2,    0x2,    0x2,    0x3a,   0x484,  0x3,
      0x2,   0x2,    0x2,    0x3c,   0x4b6,  0x3,    0x2,    0x2,    0x2,
      0x3e,  0x4c5,  0x3,    0x2,    0x2,    0x2,    0x40,   0x4c7,  0x3,
      0x2,   0x2,    0x2,    0x42,   0x4d0,  0x3,    0x2,    0x2,    0x2,
      0x44,  0x4de,  0x3,    0x2,    0x2,    0x2,    0x46,   0x4e0,  0x3,
      0x2,   0x2,    0x2,    0x48,   0x507,  0x3,    0x2,    0x2,    0x2,
      0x4a,  0x517,  0x3,    0x2,    0x2,    0x2,    0x4c,   0x519,  0x3,
      0x2,   0x2,    0x2,    0x4e,   0x522,  0x3,    0x2,    0x2,    0x2,
      0x50,  0x524,  0x3,    0x2,    0x2,    0x2,    0x52,   0x52e,  0x3,
      0x2,   0x2,    0x2,    0x54,   0x559,  0x3,    0x2,    0x2,    0x2,
      0x56,  0x55b,  0x3,    0x2,    0x2,    0x2,    0x58,   0x564,  0x3,
      0x2,   0x2,    0x2,    0x5a,   0x5ae,  0x3,    0x2,    0x2,    0x2,
      0x5c,  0x5b4,  0x3,    0x2,    0x2,    0x2,    0x5e,   0x6b6,  0x3,
      0x2,   0x2,    0x2,    0x60,   0x6cb,  0x3,    0x2,    0x2,    0x2,
      0x62,  0x6d1,  0x3,    0x2,    0x2,    0x2,    0x64,   0x6d9,  0x3,
      0x2,   0x2,    0x2,    0x66,   0x6db,  0x3,    0x2,    0x2,    0x2,
      0x68,  0x6dd,  0x3,    0x2,    0x2,    0x2,    0x6a,   0x6df,  0x3,
      0x2,   0x2,    0x2,    0x6c,   0x6e1,  0x3,    0x2,    0x2,    0x2,
      0x6e,  0x6eb,  0x3,    0x2,    0x2,    0x2,    0x70,   0x6ed,  0x3,
      0x2,   0x2,    0x2,    0x72,   0x6ef,  0x3,    0x2,    0x2,    0x2,
      0x74,  0x72b,  0x3,    0x2,    0x2,    0x2,    0x76,   0x736,  0x3,
      0x2,   0x2,    0x2,    0x78,   0x73c,  0x3,    0x2,    0x2,    0x2,
      0x7a,  0x73e,  0x3,    0x2,    0x2,    0x2,    0x7c,   0x743,  0x3,
      0x2,   0x2,    0x2,    0x7e,   0x749,  0x3,    0x2,    0x2,    0x2,
      0x80,  0x780,  0x3,    0x2,    0x2,    0x2,    0x82,   0x78b,  0x3,
      0x2,   0x2,    0x2,    0x84,   0x78d,  0x3,    0x2,    0x2,    0x2,
      0x86,  0x795,  0x3,    0x2,    0x2,    0x2,    0x88,   0x79c,  0x3,
      0x2,   0x2,    0x2,    0x8a,   0x7a5,  0x3,    0x2,    0x2,    0x2,
      0x8c,  0x7ac,  0x3,    0x2,    0x2,    0x2,    0x8e,   0x7b2,  0x3,
      0x2,   0x2,    0x2,    0x90,   0x7b4,  0x3,    0x2,    0x2,    0x2,
      0x92,  0x7bc,  0x3,    0x2,    0x2,    0x2,    0x94,   0x7c4,  0x3,
      0x2,   0x2,    0x2,    0x96,   0x7c9,  0x3,    0x2,    0x2,    0x2,
      0x98,  0x7d0,  0x3,    0x2,    0x2,    0x2,    0x9a,   0x7d2,  0x3,
      0x2,   0x2,    0x2,    0x9c,   0x7df,  0x3,    0x2,    0x2,    0x2,
      0x9e,  0x7e4,  0x3,    0x2,    0x2,    0x2,    0xa0,   0x7e8,  0x3,
      0x2,   0x2,    0x2,    0xa2,   0x7ea,  0x3,    0x2,    0x2,    0x2,
      0xa4,  0x7ee,  0x3,    0x2,    0x2,    0x2,    0xa6,   0x7f6,  0x3,
      0x2,   0x2,    0x2,    0xa8,   0x7fb,  0x3,    0x2,    0x2,    0x2,
      0xaa,  0x801,  0x3,    0x2,    0x2,    0x2,    0xac,   0x806,  0x3,
      0x2,   0x2,    0x2,    0xae,   0x808,  0x3,    0x2,    0x2,    0x2,
      0xb0,  0x80d,  0x3,    0x2,    0x2,    0x2,    0xb2,   0x80f,  0x3,
      0x2,   0x2,    0x2,    0xb4,   0xb5,   0x5,    0x8,    0x5,    0x2,
      0xb5,  0xb6,   0x7,    0x2,    0x2,    0x3,    0xb6,   0x3,    0x3,
      0x2,   0x2,    0x2,    0xb7,   0xb8,   0x5,    0x56,   0x2c,   0x2,
      0xb8,  0xb9,   0x7,    0x2,    0x2,    0x3,    0xb9,   0x5,    0x3,
      0x2,   0x2,    0x2,    0xba,   0xbb,   0x5,    0x22,   0x12,   0x2,
      0xbb,  0xbc,   0x7,    0x2,    0x2,    0x3,    0xbc,   0x7,    0x3,
      0x2,   0x2,    0x2,    0xbd,   0x39f,  0x5,    0xa,    0x6,    0x2,
      0xbe,  0xbf,   0x7,    0xd9,   0x2,    0x2,    0xbf,   0x39f,  0x5,
      0x9c,  0x4f,   0x2,    0xc0,   0xc1,   0x7,    0xd9,   0x2,    0x2,
      0xc1,  0xc2,   0x5,    0x9c,   0x4f,   0x2,    0xc2,   0xc3,   0x7,
      0x3,   0x2,    0x2,    0xc3,   0xc4,   0x5,    0x9c,   0x4f,   0x2,
      0xc4,  0x39f,  0x3,    0x2,    0x2,    0x2,    0xc5,   0xc6,   0x7,
      0x28,  0x2,    0x2,    0xc6,   0xca,   0x7,    0xb2,   0x2,    0x2,
      0xc7,  0xc8,   0x7,    0x5e,   0x2,    0x2,    0xc8,   0xc9,   0x7,
      0x86,  0x2,    0x2,    0xc9,   0xcb,   0x7,    0x46,   0x2,    0x2,
      0xca,  0xc7,   0x3,    0x2,    0x2,    0x2,    0xca,   0xcb,   0x3,
      0x2,   0x2,    0x2,    0xcb,   0xcc,   0x3,    0x2,    0x2,    0x2,
      0xcc,  0xcf,   0x5,    0x90,   0x49,   0x2,    0xcd,   0xce,   0x7,
      0xe3,  0x2,    0x2,    0xce,   0xd0,   0x5,    0x14,   0xb,    0x2,
      0xcf,  0xcd,   0x3,    0x2,    0x2,    0x2,    0xcf,   0xd0,   0x3,
      0x2,   0x2,    0x2,    0xd0,   0x39f,  0x3,    0x2,    0x2,    0x2,
      0xd1,  0xd2,   0x7,    0x3d,   0x2,    0x2,    0xd2,   0xd5,   0x7,
      0xb2,  0x2,    0x2,    0xd3,   0xd4,   0x7,    0x5e,   0x2,    0x2,
      0xd4,  0xd6,   0x7,    0x46,   0x2,    0x2,    0xd5,   0xd3,   0x3,
      0x2,   0x2,    0x2,    0xd5,   0xd6,   0x3,    0x2,    0x2,    0x2,
      0xd6,  0xd7,   0x3,    0x2,    0x2,    0x2,    0xd7,   0xd9,   0x5,
      0x90,  0x49,   0x2,    0xd8,   0xda,   0x9,    0x2,    0x2,    0x2,
      0xd9,  0xd8,   0x3,    0x2,    0x2,    0x2,    0xd9,   0xda,   0x3,
      0x2,   0x2,    0x2,    0xda,   0x39f,  0x3,    0x2,    0x2,    0x2,
      0xdb,  0xdc,   0x7,    0x10,   0x2,    0x2,    0xdc,   0xdd,   0x7,
      0xb2,  0x2,    0x2,    0xdd,   0xde,   0x5,    0x90,   0x49,   0x2,
      0xde,  0xdf,   0x7,    0xa2,   0x2,    0x2,    0xdf,   0xe0,   0x7,
      0xcc,  0x2,    0x2,    0xe0,   0xe1,   0x5,    0x9c,   0x4f,   0x2,
      0xe1,  0x39f,  0x3,    0x2,    0x2,    0x2,    0xe2,   0xe3,   0x7,
      0x28,  0x2,    0x2,    0xe3,   0xe7,   0x7,    0xc4,   0x2,    0x2,
      0xe4,  0xe5,   0x7,    0x5e,   0x2,    0x2,    0xe5,   0xe6,   0x7,
      0x86,  0x2,    0x2,    0xe6,   0xe8,   0x7,    0x46,   0x2,    0x2,
      0xe7,  0xe4,   0x3,    0x2,    0x2,    0x2,    0xe7,   0xe8,   0x3,
      0x2,   0x2,    0x2,    0xe8,   0xe9,   0x3,    0x2,    0x2,    0x2,
      0xe9,  0xeb,   0x5,    0x90,   0x49,   0x2,    0xea,   0xec,   0x5,
      0x52,  0x2a,   0x2,    0xeb,   0xea,   0x3,    0x2,    0x2,    0x2,
      0xeb,  0xec,   0x3,    0x2,    0x2,    0x2,    0xec,   0xef,   0x3,
      0x2,   0x2,    0x2,    0xed,   0xee,   0x7,    0x24,   0x2,    0x2,
      0xee,  0xf0,   0x5,    0x60,   0x31,   0x2,    0xef,   0xed,   0x3,
      0x2,   0x2,    0x2,    0xef,   0xf0,   0x3,    0x2,    0x2,    0x2,
      0xf0,  0xf3,   0x3,    0x2,    0x2,    0x2,    0xf1,   0xf2,   0x7,
      0xe3,  0x2,    0x2,    0xf2,   0xf4,   0x5,    0x14,   0xb,    0x2,
      0xf3,  0xf1,   0x3,    0x2,    0x2,    0x2,    0xf3,   0xf4,   0x3,
      0x2,   0x2,    0x2,    0xf4,   0xf5,   0x3,    0x2,    0x2,    0x2,
      0xf5,  0xfb,   0x7,    0x15,   0x2,    0x2,    0xf6,   0xfc,   0x5,
      0xa,   0x6,    0x2,    0xf7,   0xf8,   0x7,    0x4,    0x2,    0x2,
      0xf8,  0xf9,   0x5,    0xa,    0x6,    0x2,    0xf9,   0xfa,   0x7,
      0x5,   0x2,    0x2,    0xfa,   0xfc,   0x3,    0x2,    0x2,    0x2,
      0xfb,  0xf6,   0x3,    0x2,    0x2,    0x2,    0xfb,   0xf7,   0x3,
      0x2,   0x2,    0x2,    0xfc,   0x102,  0x3,    0x2,    0x2,    0x2,
      0xfd,  0xff,   0x7,    0xe3,   0x2,    0x2,    0xfe,   0x100,  0x7,
      0x83,  0x2,    0x2,    0xff,   0xfe,   0x3,    0x2,    0x2,    0x2,
      0xff,  0x100,  0x3,    0x2,    0x2,    0x2,    0x100,  0x101,  0x3,
      0x2,   0x2,    0x2,    0x101,  0x103,  0x7,    0x31,   0x2,    0x2,
      0x102, 0xfd,   0x3,    0x2,    0x2,    0x2,    0x102,  0x103,  0x3,
      0x2,   0x2,    0x2,    0x103,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x104, 0x105,  0x7,    0x28,   0x2,    0x2,    0x105,  0x109,  0x7,
      0xc4,  0x2,    0x2,    0x106,  0x107,  0x7,    0x5e,   0x2,    0x2,
      0x107, 0x108,  0x7,    0x86,   0x2,    0x2,    0x108,  0x10a,  0x7,
      0x46,  0x2,    0x2,    0x109,  0x106,  0x3,    0x2,    0x2,    0x2,
      0x109, 0x10a,  0x3,    0x2,    0x2,    0x2,    0x10a,  0x10b,  0x3,
      0x2,   0x2,    0x2,    0x10b,  0x10c,  0x5,    0x90,   0x49,   0x2,
      0x10c, 0x10d,  0x7,    0x4,    0x2,    0x2,    0x10d,  0x112,  0x5,
      0xe,   0x8,    0x2,    0x10e,  0x10f,  0x7,    0x6,    0x2,    0x2,
      0x10f, 0x111,  0x5,    0xe,    0x8,    0x2,    0x110,  0x10e,  0x3,
      0x2,   0x2,    0x2,    0x111,  0x114,  0x3,    0x2,    0x2,    0x2,
      0x112, 0x110,  0x3,    0x2,    0x2,    0x2,    0x112,  0x113,  0x3,
      0x2,   0x2,    0x2,    0x113,  0x115,  0x3,    0x2,    0x2,    0x2,
      0x114, 0x112,  0x3,    0x2,    0x2,    0x2,    0x115,  0x118,  0x7,
      0x5,   0x2,    0x2,    0x116,  0x117,  0x7,    0x24,   0x2,    0x2,
      0x117, 0x119,  0x5,    0x60,   0x31,   0x2,    0x118,  0x116,  0x3,
      0x2,   0x2,    0x2,    0x118,  0x119,  0x3,    0x2,    0x2,    0x2,
      0x119, 0x11c,  0x3,    0x2,    0x2,    0x2,    0x11a,  0x11b,  0x7,
      0xe3,  0x2,    0x2,    0x11b,  0x11d,  0x5,    0x14,   0xb,    0x2,
      0x11c, 0x11a,  0x3,    0x2,    0x2,    0x2,    0x11c,  0x11d,  0x3,
      0x2,   0x2,    0x2,    0x11d,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x11e, 0x11f,  0x7,    0x3d,   0x2,    0x2,    0x11f,  0x122,  0x7,
      0xc4,  0x2,    0x2,    0x120,  0x121,  0x7,    0x5e,   0x2,    0x2,
      0x121, 0x123,  0x7,    0x46,   0x2,    0x2,    0x122,  0x120,  0x3,
      0x2,   0x2,    0x2,    0x122,  0x123,  0x3,    0x2,    0x2,    0x2,
      0x123, 0x124,  0x3,    0x2,    0x2,    0x2,    0x124,  0x39f,  0x5,
      0x90,  0x49,   0x2,    0x125,  0x126,  0x7,    0x64,   0x2,    0x2,
      0x126, 0x127,  0x7,    0x67,   0x2,    0x2,    0x127,  0x129,  0x5,
      0x90,  0x49,   0x2,    0x128,  0x12a,  0x5,    0x52,   0x2a,   0x2,
      0x129, 0x128,  0x3,    0x2,    0x2,    0x2,    0x129,  0x12a,  0x3,
      0x2,   0x2,    0x2,    0x12a,  0x12b,  0x3,    0x2,    0x2,    0x2,
      0x12b, 0x12c,  0x5,    0xa,    0x6,    0x2,    0x12c,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x12d,  0x12e,  0x7,    0x36,   0x2,    0x2,
      0x12e, 0x12f,  0x7,    0x51,   0x2,    0x2,    0x12f,  0x132,  0x5,
      0x90,  0x49,   0x2,    0x130,  0x131,  0x7,    0xe2,   0x2,    0x2,
      0x131, 0x133,  0x5,    0x58,   0x2d,   0x2,    0x132,  0x130,  0x3,
      0x2,   0x2,    0x2,    0x132,  0x133,  0x3,    0x2,    0x2,    0x2,
      0x133, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x134,  0x135,  0x7,
      0xcf,  0x2,    0x2,    0x135,  0x136,  0x7,    0xc4,   0x2,    0x2,
      0x136, 0x39f,  0x5,    0x90,   0x49,   0x2,    0x137,  0x138,  0x7,
      0x10,  0x2,    0x2,    0x138,  0x13b,  0x7,    0xc4,   0x2,    0x2,
      0x139, 0x13a,  0x7,    0x5e,   0x2,    0x2,    0x13a,  0x13c,  0x7,
      0x46,  0x2,    0x2,    0x13b,  0x139,  0x3,    0x2,    0x2,    0x2,
      0x13b, 0x13c,  0x3,    0x2,    0x2,    0x2,    0x13c,  0x13d,  0x3,
      0x2,   0x2,    0x2,    0x13d,  0x13e,  0x5,    0x90,   0x49,   0x2,
      0x13e, 0x13f,  0x7,    0xa2,   0x2,    0x2,    0x13f,  0x140,  0x7,
      0xcc,  0x2,    0x2,    0x140,  0x141,  0x5,    0x90,   0x49,   0x2,
      0x141, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x142,  0x143,  0x7,
      0x10,  0x2,    0x2,    0x143,  0x146,  0x7,    0xc4,   0x2,    0x2,
      0x144, 0x145,  0x7,    0x5e,   0x2,    0x2,    0x145,  0x147,  0x7,
      0x46,  0x2,    0x2,    0x146,  0x144,  0x3,    0x2,    0x2,    0x2,
      0x146, 0x147,  0x3,    0x2,    0x2,    0x2,    0x147,  0x148,  0x3,
      0x2,   0x2,    0x2,    0x148,  0x149,  0x5,    0x90,   0x49,   0x2,
      0x149, 0x14a,  0x7,    0xa2,   0x2,    0x2,    0x14a,  0x14d,  0x7,
      0x22,  0x2,    0x2,    0x14b,  0x14c,  0x7,    0x5e,   0x2,    0x2,
      0x14c, 0x14e,  0x7,    0x46,   0x2,    0x2,    0x14d,  0x14b,  0x3,
      0x2,   0x2,    0x2,    0x14d,  0x14e,  0x3,    0x2,    0x2,    0x2,
      0x14e, 0x14f,  0x3,    0x2,    0x2,    0x2,    0x14f,  0x150,  0x5,
      0x9c,  0x4f,   0x2,    0x150,  0x151,  0x7,    0xcc,   0x2,    0x2,
      0x151, 0x152,  0x5,    0x9c,   0x4f,   0x2,    0x152,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x153,  0x154,  0x7,    0x10,   0x2,    0x2,
      0x154, 0x157,  0x7,    0xc4,   0x2,    0x2,    0x155,  0x156,  0x7,
      0x5e,  0x2,    0x2,    0x156,  0x158,  0x7,    0x46,   0x2,    0x2,
      0x157, 0x155,  0x3,    0x2,    0x2,    0x2,    0x157,  0x158,  0x3,
      0x2,   0x2,    0x2,    0x158,  0x159,  0x3,    0x2,    0x2,    0x2,
      0x159, 0x15a,  0x5,    0x90,   0x49,   0x2,    0x15a,  0x15b,  0x7,
      0x3d,  0x2,    0x2,    0x15b,  0x15e,  0x7,    0x22,   0x2,    0x2,
      0x15c, 0x15d,  0x7,    0x5e,   0x2,    0x2,    0x15d,  0x15f,  0x7,
      0x46,  0x2,    0x2,    0x15e,  0x15c,  0x3,    0x2,    0x2,    0x2,
      0x15e, 0x15f,  0x3,    0x2,    0x2,    0x2,    0x15f,  0x160,  0x3,
      0x2,   0x2,    0x2,    0x160,  0x161,  0x5,    0x90,   0x49,   0x2,
      0x161, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x162,  0x163,  0x7,
      0x10,  0x2,    0x2,    0x163,  0x166,  0x7,    0xc4,   0x2,    0x2,
      0x164, 0x165,  0x7,    0x5e,   0x2,    0x2,    0x165,  0x167,  0x7,
      0x46,  0x2,    0x2,    0x166,  0x164,  0x3,    0x2,    0x2,    0x2,
      0x166, 0x167,  0x3,    0x2,    0x2,    0x2,    0x167,  0x168,  0x3,
      0x2,   0x2,    0x2,    0x168,  0x169,  0x5,    0x90,   0x49,   0x2,
      0x169, 0x16a,  0x7,    0xd,    0x2,    0x2,    0x16a,  0x16e,  0x7,
      0x22,  0x2,    0x2,    0x16b,  0x16c,  0x7,    0x5e,   0x2,    0x2,
      0x16c, 0x16d,  0x7,    0x86,   0x2,    0x2,    0x16d,  0x16f,  0x7,
      0x46,  0x2,    0x2,    0x16e,  0x16b,  0x3,    0x2,    0x2,    0x2,
      0x16e, 0x16f,  0x3,    0x2,    0x2,    0x2,    0x16f,  0x170,  0x3,
      0x2,   0x2,    0x2,    0x170,  0x171,  0x5,    0x10,   0x9,    0x2,
      0x171, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x172,  0x173,  0x7,
      0x10,  0x2,    0x2,    0x173,  0x176,  0x7,    0xc4,   0x2,    0x2,
      0x174, 0x175,  0x7,    0x5e,   0x2,    0x2,    0x175,  0x177,  0x7,
      0x46,  0x2,    0x2,    0x176,  0x174,  0x3,    0x2,    0x2,    0x2,
      0x176, 0x177,  0x3,    0x2,    0x2,    0x2,    0x177,  0x178,  0x3,
      0x2,   0x2,    0x2,    0x178,  0x179,  0x5,    0x90,   0x49,   0x2,
      0x179, 0x17a,  0x7,    0xd,    0x2,    0x2,    0x17a,  0x17b,  0x5,
      0xa0,  0x51,   0x2,    0x17b,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x17c, 0x17d,  0x7,    0x10,   0x2,    0x2,    0x17d,  0x180,  0x7,
      0xc4,  0x2,    0x2,    0x17e,  0x17f,  0x7,    0x5e,   0x2,    0x2,
      0x17f, 0x181,  0x7,    0x46,   0x2,    0x2,    0x180,  0x17e,  0x3,
      0x2,   0x2,    0x2,    0x180,  0x181,  0x3,    0x2,    0x2,    0x2,
      0x181, 0x182,  0x3,    0x2,    0x2,    0x2,    0x182,  0x183,  0x5,
      0x90,  0x49,   0x2,    0x183,  0x184,  0x7,    0x3d,   0x2,    0x2,
      0x184, 0x187,  0x7,    0x27,   0x2,    0x2,    0x185,  0x186,  0x7,
      0x5e,  0x2,    0x2,    0x186,  0x188,  0x7,    0x46,   0x2,    0x2,
      0x187, 0x185,  0x3,    0x2,    0x2,    0x2,    0x187,  0x188,  0x3,
      0x2,   0x2,    0x2,    0x188,  0x189,  0x3,    0x2,    0x2,    0x2,
      0x189, 0x18a,  0x5,    0x9c,   0x4f,   0x2,    0x18a,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x18b,  0x18c,  0x7,    0x10,   0x2,    0x2,
      0x18c, 0x18f,  0x7,    0xc4,   0x2,    0x2,    0x18d,  0x18e,  0x7,
      0x5e,  0x2,    0x2,    0x18e,  0x190,  0x7,    0x46,   0x2,    0x2,
      0x18f, 0x18d,  0x3,    0x2,    0x2,    0x2,    0x18f,  0x190,  0x3,
      0x2,   0x2,    0x2,    0x190,  0x191,  0x3,    0x2,    0x2,    0x2,
      0x191, 0x192,  0x5,    0x90,   0x49,   0x2,    0x192,  0x194,  0x7,
      0x10,  0x2,    0x2,    0x193,  0x195,  0x7,    0x22,   0x2,    0x2,
      0x194, 0x193,  0x3,    0x2,    0x2,    0x2,    0x194,  0x195,  0x3,
      0x2,   0x2,    0x2,    0x195,  0x196,  0x3,    0x2,    0x2,    0x2,
      0x196, 0x197,  0x5,    0x9c,   0x4f,   0x2,    0x197,  0x198,  0x7,
      0xb9,  0x2,    0x2,    0x198,  0x199,  0x7,    0x86,   0x2,    0x2,
      0x199, 0x19a,  0x7,    0x87,   0x2,    0x2,    0x19a,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x19b,  0x19c,  0x7,    0x10,   0x2,    0x2,
      0x19c, 0x19f,  0x7,    0xc4,   0x2,    0x2,    0x19d,  0x19e,  0x7,
      0x5e,  0x2,    0x2,    0x19e,  0x1a0,  0x7,    0x46,   0x2,    0x2,
      0x19f, 0x19d,  0x3,    0x2,    0x2,    0x2,    0x19f,  0x1a0,  0x3,
      0x2,   0x2,    0x2,    0x1a0,  0x1a1,  0x3,    0x2,    0x2,    0x2,
      0x1a1, 0x1a2,  0x5,    0x90,   0x49,   0x2,    0x1a2,  0x1a4,  0x7,
      0x10,  0x2,    0x2,    0x1a3,  0x1a5,  0x7,    0x22,   0x2,    0x2,
      0x1a4, 0x1a3,  0x3,    0x2,    0x2,    0x2,    0x1a4,  0x1a5,  0x3,
      0x2,   0x2,    0x2,    0x1a5,  0x1a6,  0x3,    0x2,    0x2,    0x2,
      0x1a6, 0x1a7,  0x5,    0x9c,   0x4f,   0x2,    0x1a7,  0x1a8,  0x7,
      0x3d,  0x2,    0x2,    0x1a8,  0x1a9,  0x7,    0x86,   0x2,    0x2,
      0x1a9, 0x1aa,  0x7,    0x87,   0x2,    0x2,    0x1aa,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x1ab,  0x1ac,  0x7,    0x10,   0x2,    0x2,
      0x1ac, 0x1af,  0x7,    0xc4,   0x2,    0x2,    0x1ad,  0x1ae,  0x7,
      0x5e,  0x2,    0x2,    0x1ae,  0x1b0,  0x7,    0x46,   0x2,    0x2,
      0x1af, 0x1ad,  0x3,    0x2,    0x2,    0x2,    0x1af,  0x1b0,  0x3,
      0x2,   0x2,    0x2,    0x1b0,  0x1b1,  0x3,    0x2,    0x2,    0x2,
      0x1b1, 0x1b2,  0x5,    0x90,   0x49,   0x2,    0x1b2,  0x1b3,  0x7,
      0xb9,  0x2,    0x2,    0x1b3,  0x1b4,  0x7,    0x9c,   0x2,    0x2,
      0x1b4, 0x1b5,  0x5,    0x14,   0xb,    0x2,    0x1b5,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x1b6,  0x1b7,  0x7,    0x11,   0x2,    0x2,
      0x1b7, 0x1ba,  0x5,    0x90,   0x49,   0x2,    0x1b8,  0x1b9,  0x7,
      0xe3,  0x2,    0x2,    0x1b9,  0x1bb,  0x5,    0x14,   0xb,    0x2,
      0x1ba, 0x1b8,  0x3,    0x2,    0x2,    0x2,    0x1ba,  0x1bb,  0x3,
      0x2,   0x2,    0x2,    0x1bb,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x1bc, 0x1bd,  0x7,    0x28,   0x2,    0x2,    0x1bd,  0x1be,  0x7,
      0xd1,  0x2,    0x2,    0x1be,  0x1bf,  0x5,    0x90,   0x49,   0x2,
      0x1bf, 0x1cc,  0x7,    0x15,   0x2,    0x2,    0x1c0,  0x1c1,  0x7,
      0x4,   0x2,    0x2,    0x1c1,  0x1c6,  0x5,    0x18,   0xd,    0x2,
      0x1c2, 0x1c3,  0x7,    0x6,    0x2,    0x2,    0x1c3,  0x1c5,  0x5,
      0x18,  0xd,    0x2,    0x1c4,  0x1c2,  0x3,    0x2,    0x2,    0x2,
      0x1c5, 0x1c8,  0x3,    0x2,    0x2,    0x2,    0x1c6,  0x1c4,  0x3,
      0x2,   0x2,    0x2,    0x1c6,  0x1c7,  0x3,    0x2,    0x2,    0x2,
      0x1c7, 0x1c9,  0x3,    0x2,    0x2,    0x2,    0x1c8,  0x1c6,  0x3,
      0x2,   0x2,    0x2,    0x1c9,  0x1ca,  0x7,    0x5,    0x2,    0x2,
      0x1ca, 0x1cd,  0x3,    0x2,    0x2,    0x2,    0x1cb,  0x1cd,  0x5,
      0x74,  0x3b,   0x2,    0x1cc,  0x1c0,  0x3,    0x2,    0x2,    0x2,
      0x1cc, 0x1cb,  0x3,    0x2,    0x2,    0x2,    0x1cd,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x1ce,  0x1d1,  0x7,    0x28,   0x2,    0x2,
      0x1cf, 0x1d0,  0x7,    0x8f,   0x2,    0x2,    0x1d0,  0x1d2,  0x7,
      0xa4,  0x2,    0x2,    0x1d1,  0x1cf,  0x3,    0x2,    0x2,    0x2,
      0x1d1, 0x1d2,  0x3,    0x2,    0x2,    0x2,    0x1d2,  0x1d3,  0x3,
      0x2,   0x2,    0x2,    0x1d3,  0x1d4,  0x7,    0xe0,   0x2,    0x2,
      0x1d4, 0x1d7,  0x5,    0x90,   0x49,   0x2,    0x1d5,  0x1d6,  0x7,
      0xb5,  0x2,    0x2,    0x1d6,  0x1d8,  0x9,    0x3,    0x2,    0x2,
      0x1d7, 0x1d5,  0x3,    0x2,    0x2,    0x2,    0x1d7,  0x1d8,  0x3,
      0x2,   0x2,    0x2,    0x1d8,  0x1d9,  0x3,    0x2,    0x2,    0x2,
      0x1d9, 0x1da,  0x7,    0x15,   0x2,    0x2,    0x1da,  0x1db,  0x5,
      0xa,   0x6,    0x2,    0x1db,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x1dc, 0x1dd,  0x7,    0x10,   0x2,    0x2,    0x1dd,  0x1e0,  0x7,
      0xe0,  0x2,    0x2,    0x1de,  0x1df,  0x7,    0x5e,   0x2,    0x2,
      0x1df, 0x1e1,  0x7,    0x46,   0x2,    0x2,    0x1e0,  0x1de,  0x3,
      0x2,   0x2,    0x2,    0x1e0,  0x1e1,  0x3,    0x2,    0x2,    0x2,
      0x1e1, 0x1e2,  0x3,    0x2,    0x2,    0x2,    0x1e2,  0x1e3,  0x5,
      0x90,  0x49,   0x2,    0x1e3,  0x1e4,  0x7,    0xa2,   0x2,    0x2,
      0x1e4, 0x1e5,  0x7,    0xcc,   0x2,    0x2,    0x1e5,  0x1e6,  0x5,
      0x90,  0x49,   0x2,    0x1e6,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x1e7, 0x1e8,  0x7,    0x3d,   0x2,    0x2,    0x1e8,  0x1eb,  0x7,
      0xe0,  0x2,    0x2,    0x1e9,  0x1ea,  0x7,    0x5e,   0x2,    0x2,
      0x1ea, 0x1ec,  0x7,    0x46,   0x2,    0x2,    0x1eb,  0x1e9,  0x3,
      0x2,   0x2,    0x2,    0x1eb,  0x1ec,  0x3,    0x2,    0x2,    0x2,
      0x1ec, 0x1ed,  0x3,    0x2,    0x2,    0x2,    0x1ed,  0x39f,  0x5,
      0x90,  0x49,   0x2,    0x1ee,  0x1ef,  0x7,    0x28,   0x2,    0x2,
      0x1ef, 0x1f0,  0x7,    0x7a,   0x2,    0x2,    0x1f0,  0x1f4,  0x7,
      0xe0,  0x2,    0x2,    0x1f1,  0x1f2,  0x7,    0x5e,   0x2,    0x2,
      0x1f2, 0x1f3,  0x7,    0x86,   0x2,    0x2,    0x1f3,  0x1f5,  0x7,
      0x46,  0x2,    0x2,    0x1f4,  0x1f1,  0x3,    0x2,    0x2,    0x2,
      0x1f4, 0x1f5,  0x3,    0x2,    0x2,    0x2,    0x1f5,  0x1f6,  0x3,
      0x2,   0x2,    0x2,    0x1f6,  0x1f9,  0x5,    0x90,   0x49,   0x2,
      0x1f7, 0x1f8,  0x7,    0x24,   0x2,    0x2,    0x1f8,  0x1fa,  0x5,
      0x60,  0x31,   0x2,    0x1f9,  0x1f7,  0x3,    0x2,    0x2,    0x2,
      0x1f9, 0x1fa,  0x3,    0x2,    0x2,    0x2,    0x1fa,  0x1fd,  0x3,
      0x2,   0x2,    0x2,    0x1fb,  0x1fc,  0x7,    0xe3,   0x2,    0x2,
      0x1fc, 0x1fe,  0x5,    0x14,   0xb,    0x2,    0x1fd,  0x1fb,  0x3,
      0x2,   0x2,    0x2,    0x1fd,  0x1fe,  0x3,    0x2,    0x2,    0x2,
      0x1fe, 0x1ff,  0x3,    0x2,    0x2,    0x2,    0x1ff,  0x205,  0x7,
      0x15,  0x2,    0x2,    0x200,  0x206,  0x5,    0xa,    0x6,    0x2,
      0x201, 0x202,  0x7,    0x4,    0x2,    0x2,    0x202,  0x203,  0x5,
      0xa,   0x6,    0x2,    0x203,  0x204,  0x7,    0x5,    0x2,    0x2,
      0x204, 0x206,  0x3,    0x2,    0x2,    0x2,    0x205,  0x200,  0x3,
      0x2,   0x2,    0x2,    0x205,  0x201,  0x3,    0x2,    0x2,    0x2,
      0x206, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x207,  0x208,  0x7,
      0x3d,  0x2,    0x2,    0x208,  0x209,  0x7,    0x7a,   0x2,    0x2,
      0x209, 0x20c,  0x7,    0xe0,   0x2,    0x2,    0x20a,  0x20b,  0x7,
      0x5e,  0x2,    0x2,    0x20b,  0x20d,  0x7,    0x46,   0x2,    0x2,
      0x20c, 0x20a,  0x3,    0x2,    0x2,    0x2,    0x20c,  0x20d,  0x3,
      0x2,   0x2,    0x2,    0x20d,  0x20e,  0x3,    0x2,    0x2,    0x2,
      0x20e, 0x39f,  0x5,    0x90,   0x49,   0x2,    0x20f,  0x210,  0x7,
      0xa0,  0x2,    0x2,    0x210,  0x211,  0x7,    0x7a,   0x2,    0x2,
      0x211, 0x212,  0x7,    0xe0,   0x2,    0x2,    0x212,  0x213,  0x5,
      0x90,  0x49,   0x2,    0x213,  0x214,  0x7,    0xe2,   0x2,    0x2,
      0x214, 0x215,  0x5,    0x58,   0x2d,   0x2,    0x215,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x216,  0x219,  0x7,    0x28,   0x2,    0x2,
      0x217, 0x218,  0x7,    0x8f,   0x2,    0x2,    0x218,  0x21a,  0x7,
      0xa4,  0x2,    0x2,    0x219,  0x217,  0x3,    0x2,    0x2,    0x2,
      0x219, 0x21a,  0x3,    0x2,    0x2,    0x2,    0x21a,  0x21c,  0x3,
      0x2,   0x2,    0x2,    0x21b,  0x21d,  0x7,    0xc7,   0x2,    0x2,
      0x21c, 0x21b,  0x3,    0x2,    0x2,    0x2,    0x21c,  0x21d,  0x3,
      0x2,   0x2,    0x2,    0x21d,  0x21e,  0x3,    0x2,    0x2,    0x2,
      0x21e, 0x21f,  0x7,    0x53,   0x2,    0x2,    0x21f,  0x220,  0x5,
      0x90,  0x49,   0x2,    0x220,  0x229,  0x7,    0x4,    0x2,    0x2,
      0x221, 0x226,  0x5,    0x18,   0xd,    0x2,    0x222,  0x223,  0x7,
      0x6,   0x2,    0x2,    0x223,  0x225,  0x5,    0x18,   0xd,    0x2,
      0x224, 0x222,  0x3,    0x2,    0x2,    0x2,    0x225,  0x228,  0x3,
      0x2,   0x2,    0x2,    0x226,  0x224,  0x3,    0x2,    0x2,    0x2,
      0x226, 0x227,  0x3,    0x2,    0x2,    0x2,    0x227,  0x22a,  0x3,
      0x2,   0x2,    0x2,    0x228,  0x226,  0x3,    0x2,    0x2,    0x2,
      0x229, 0x221,  0x3,    0x2,    0x2,    0x2,    0x229,  0x22a,  0x3,
      0x2,   0x2,    0x2,    0x22a,  0x22b,  0x3,    0x2,    0x2,    0x2,
      0x22b, 0x22c,  0x7,    0x5,    0x2,    0x2,    0x22c,  0x22d,  0x7,
      0xa9,  0x2,    0x2,    0x22d,  0x230,  0x5,    0x74,   0x3b,   0x2,
      0x22e, 0x22f,  0x7,    0x24,   0x2,    0x2,    0x22f,  0x231,  0x5,
      0x60,  0x31,   0x2,    0x230,  0x22e,  0x3,    0x2,    0x2,    0x2,
      0x230, 0x231,  0x3,    0x2,    0x2,    0x2,    0x231,  0x232,  0x3,
      0x2,   0x2,    0x2,    0x232,  0x233,  0x5,    0x1a,   0xe,    0x2,
      0x233, 0x234,  0x5,    0x22,   0x12,   0x2,    0x234,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x235,  0x236,  0x7,    0x10,   0x2,    0x2,
      0x236, 0x237,  0x7,    0x53,   0x2,    0x2,    0x237,  0x239,  0x5,
      0x90,  0x49,   0x2,    0x238,  0x23a,  0x5,    0x72,   0x3a,   0x2,
      0x239, 0x238,  0x3,    0x2,    0x2,    0x2,    0x239,  0x23a,  0x3,
      0x2,   0x2,    0x2,    0x23a,  0x23b,  0x3,    0x2,    0x2,    0x2,
      0x23b, 0x23c,  0x5,    0x1e,   0x10,   0x2,    0x23c,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x23d,  0x23f,  0x7,    0x3d,   0x2,    0x2,
      0x23e, 0x240,  0x7,    0xc7,   0x2,    0x2,    0x23f,  0x23e,  0x3,
      0x2,   0x2,    0x2,    0x23f,  0x240,  0x3,    0x2,    0x2,    0x2,
      0x240, 0x241,  0x3,    0x2,    0x2,    0x2,    0x241,  0x244,  0x7,
      0x53,  0x2,    0x2,    0x242,  0x243,  0x7,    0x5e,   0x2,    0x2,
      0x243, 0x245,  0x7,    0x46,   0x2,    0x2,    0x244,  0x242,  0x3,
      0x2,   0x2,    0x2,    0x244,  0x245,  0x3,    0x2,    0x2,    0x2,
      0x245, 0x246,  0x3,    0x2,    0x2,    0x2,    0x246,  0x248,  0x5,
      0x90,  0x49,   0x2,    0x247,  0x249,  0x5,    0x72,   0x3a,   0x2,
      0x248, 0x247,  0x3,    0x2,    0x2,    0x2,    0x248,  0x249,  0x3,
      0x2,   0x2,    0x2,    0x249,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x24a, 0x24b,  0x7,    0x1c,   0x2,    0x2,    0x24b,  0x24c,  0x5,
      0x90,  0x49,   0x2,    0x24c,  0x255,  0x7,    0x4,    0x2,    0x2,
      0x24d, 0x252,  0x5,    0x8c,   0x47,   0x2,    0x24e,  0x24f,  0x7,
      0x6,   0x2,    0x2,    0x24f,  0x251,  0x5,    0x8c,   0x47,   0x2,
      0x250, 0x24e,  0x3,    0x2,    0x2,    0x2,    0x251,  0x254,  0x3,
      0x2,   0x2,    0x2,    0x252,  0x250,  0x3,    0x2,    0x2,    0x2,
      0x252, 0x253,  0x3,    0x2,    0x2,    0x2,    0x253,  0x256,  0x3,
      0x2,   0x2,    0x2,    0x254,  0x252,  0x3,    0x2,    0x2,    0x2,
      0x255, 0x24d,  0x3,    0x2,    0x2,    0x2,    0x255,  0x256,  0x3,
      0x2,   0x2,    0x2,    0x256,  0x257,  0x3,    0x2,    0x2,    0x2,
      0x257, 0x258,  0x7,    0x5,    0x2,    0x2,    0x258,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x259,  0x25a,  0x7,    0x28,   0x2,    0x2,
      0x25a, 0x25b,  0x7,    0xac,   0x2,    0x2,    0x25b,  0x25f,  0x5,
      0x9c,  0x4f,   0x2,    0x25c,  0x25d,  0x7,    0xe3,   0x2,    0x2,
      0x25d, 0x25e,  0x7,    0xe,    0x2,    0x2,    0x25e,  0x260,  0x5,
      0x96,  0x4c,   0x2,    0x25f,  0x25c,  0x3,    0x2,    0x2,    0x2,
      0x25f, 0x260,  0x3,    0x2,    0x2,    0x2,    0x260,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x261,  0x262,  0x7,    0x3d,   0x2,    0x2,
      0x262, 0x263,  0x7,    0xac,   0x2,    0x2,    0x263,  0x39f,  0x5,
      0x9c,  0x4f,   0x2,    0x264,  0x265,  0x7,    0x55,   0x2,    0x2,
      0x265, 0x266,  0x5,    0x9a,   0x4e,   0x2,    0x266,  0x267,  0x7,
      0xcc,  0x2,    0x2,    0x267,  0x26c,  0x5,    0x98,   0x4d,   0x2,
      0x268, 0x269,  0x7,    0x6,    0x2,    0x2,    0x269,  0x26b,  0x5,
      0x98,  0x4d,   0x2,    0x26a,  0x268,  0x3,    0x2,    0x2,    0x2,
      0x26b, 0x26e,  0x3,    0x2,    0x2,    0x2,    0x26c,  0x26a,  0x3,
      0x2,   0x2,    0x2,    0x26c,  0x26d,  0x3,    0x2,    0x2,    0x2,
      0x26d, 0x272,  0x3,    0x2,    0x2,    0x2,    0x26e,  0x26c,  0x3,
      0x2,   0x2,    0x2,    0x26f,  0x270,  0x7,    0xe3,   0x2,    0x2,
      0x270, 0x271,  0x7,    0xe,    0x2,    0x2,    0x271,  0x273,  0x7,
      0x8e,  0x2,    0x2,    0x272,  0x26f,  0x3,    0x2,    0x2,    0x2,
      0x272, 0x273,  0x3,    0x2,    0x2,    0x2,    0x273,  0x277,  0x3,
      0x2,   0x2,    0x2,    0x274,  0x275,  0x7,    0x56,   0x2,    0x2,
      0x275, 0x276,  0x7,    0x1b,   0x2,    0x2,    0x276,  0x278,  0x5,
      0x96,  0x4c,   0x2,    0x277,  0x274,  0x3,    0x2,    0x2,    0x2,
      0x277, 0x278,  0x3,    0x2,    0x2,    0x2,    0x278,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x279,  0x27d,  0x7,    0xaa,   0x2,    0x2,
      0x27a, 0x27b,  0x7,    0xe,    0x2,    0x2,    0x27b,  0x27c,  0x7,
      0x8e,  0x2,    0x2,    0x27c,  0x27e,  0x7,    0x4f,   0x2,    0x2,
      0x27d, 0x27a,  0x3,    0x2,    0x2,    0x2,    0x27d,  0x27e,  0x3,
      0x2,   0x2,    0x2,    0x27e,  0x27f,  0x3,    0x2,    0x2,    0x2,
      0x27f, 0x280,  0x5,    0x9a,   0x4e,   0x2,    0x280,  0x281,  0x7,
      0x51,  0x2,    0x2,    0x281,  0x286,  0x5,    0x98,   0x4d,   0x2,
      0x282, 0x283,  0x7,    0x6,    0x2,    0x2,    0x283,  0x285,  0x5,
      0x98,  0x4d,   0x2,    0x284,  0x282,  0x3,    0x2,    0x2,    0x2,
      0x285, 0x288,  0x3,    0x2,    0x2,    0x2,    0x286,  0x284,  0x3,
      0x2,   0x2,    0x2,    0x286,  0x287,  0x3,    0x2,    0x2,    0x2,
      0x287, 0x28c,  0x3,    0x2,    0x2,    0x2,    0x288,  0x286,  0x3,
      0x2,   0x2,    0x2,    0x289,  0x28a,  0x7,    0x56,   0x2,    0x2,
      0x28a, 0x28b,  0x7,    0x1b,   0x2,    0x2,    0x28b,  0x28d,  0x5,
      0x96,  0x4c,   0x2,    0x28c,  0x289,  0x3,    0x2,    0x2,    0x2,
      0x28c, 0x28d,  0x3,    0x2,    0x2,    0x2,    0x28d,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x28e,  0x28f,  0x7,    0xb9,   0x2,    0x2,
      0x28f, 0x293,  0x7,    0xac,   0x2,    0x2,    0x290,  0x294,  0x7,
      0xf,   0x2,    0x2,    0x291,  0x294,  0x7,    0x84,   0x2,    0x2,
      0x292, 0x294,  0x5,    0x9c,   0x4f,   0x2,    0x293,  0x290,  0x3,
      0x2,   0x2,    0x2,    0x293,  0x291,  0x3,    0x2,    0x2,    0x2,
      0x293, 0x292,  0x3,    0x2,    0x2,    0x2,    0x294,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x295,  0x2a0,  0x7,    0x55,   0x2,    0x2,
      0x296, 0x29b,  0x5,    0x8e,   0x48,   0x2,    0x297,  0x298,  0x7,
      0x6,   0x2,    0x2,    0x298,  0x29a,  0x5,    0x8e,   0x48,   0x2,
      0x299, 0x297,  0x3,    0x2,    0x2,    0x2,    0x29a,  0x29d,  0x3,
      0x2,   0x2,    0x2,    0x29b,  0x299,  0x3,    0x2,    0x2,    0x2,
      0x29b, 0x29c,  0x3,    0x2,    0x2,    0x2,    0x29c,  0x2a1,  0x3,
      0x2,   0x2,    0x2,    0x29d,  0x29b,  0x3,    0x2,    0x2,    0x2,
      0x29e, 0x29f,  0x7,    0xf,    0x2,    0x2,    0x29f,  0x2a1,  0x7,
      0x9b,  0x2,    0x2,    0x2a0,  0x296,  0x3,    0x2,    0x2,    0x2,
      0x2a0, 0x29e,  0x3,    0x2,    0x2,    0x2,    0x2a1,  0x2a2,  0x3,
      0x2,   0x2,    0x2,    0x2a2,  0x2a4,  0x7,    0x8c,   0x2,    0x2,
      0x2a3, 0x2a5,  0x7,    0xc4,   0x2,    0x2,    0x2a4,  0x2a3,  0x3,
      0x2,   0x2,    0x2,    0x2a4,  0x2a5,  0x3,    0x2,    0x2,    0x2,
      0x2a5, 0x2a6,  0x3,    0x2,    0x2,    0x2,    0x2a6,  0x2a7,  0x5,
      0x90,  0x49,   0x2,    0x2a7,  0x2a8,  0x7,    0xcc,   0x2,    0x2,
      0x2a8, 0x2ac,  0x5,    0x98,   0x4d,   0x2,    0x2a9,  0x2aa,  0x7,
      0xe3,  0x2,    0x2,    0x2aa,  0x2ab,  0x7,    0x55,   0x2,    0x2,
      0x2ab, 0x2ad,  0x7,    0x8e,   0x2,    0x2,    0x2ac,  0x2a9,  0x3,
      0x2,   0x2,    0x2,    0x2ac,  0x2ad,  0x3,    0x2,    0x2,    0x2,
      0x2ad, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x2ae,  0x2b2,  0x7,
      0xaa,  0x2,    0x2,    0x2af,  0x2b0,  0x7,    0x55,   0x2,    0x2,
      0x2b0, 0x2b1,  0x7,    0x8e,   0x2,    0x2,    0x2b1,  0x2b3,  0x7,
      0x4f,  0x2,    0x2,    0x2b2,  0x2af,  0x3,    0x2,    0x2,    0x2,
      0x2b2, 0x2b3,  0x3,    0x2,    0x2,    0x2,    0x2b3,  0x2be,  0x3,
      0x2,   0x2,    0x2,    0x2b4,  0x2b9,  0x5,    0x8e,   0x48,   0x2,
      0x2b5, 0x2b6,  0x7,    0x6,    0x2,    0x2,    0x2b6,  0x2b8,  0x5,
      0x8e,  0x48,   0x2,    0x2b7,  0x2b5,  0x3,    0x2,    0x2,    0x2,
      0x2b8, 0x2bb,  0x3,    0x2,    0x2,    0x2,    0x2b9,  0x2b7,  0x3,
      0x2,   0x2,    0x2,    0x2b9,  0x2ba,  0x3,    0x2,    0x2,    0x2,
      0x2ba, 0x2bf,  0x3,    0x2,    0x2,    0x2,    0x2bb,  0x2b9,  0x3,
      0x2,   0x2,    0x2,    0x2bc,  0x2bd,  0x7,    0xf,    0x2,    0x2,
      0x2bd, 0x2bf,  0x7,    0x9b,   0x2,    0x2,    0x2be,  0x2b4,  0x3,
      0x2,   0x2,    0x2,    0x2be,  0x2bc,  0x3,    0x2,    0x2,    0x2,
      0x2bf, 0x2c0,  0x3,    0x2,    0x2,    0x2,    0x2c0,  0x2c2,  0x7,
      0x8c,  0x2,    0x2,    0x2c1,  0x2c3,  0x7,    0xc4,   0x2,    0x2,
      0x2c2, 0x2c1,  0x3,    0x2,    0x2,    0x2,    0x2c2,  0x2c3,  0x3,
      0x2,   0x2,    0x2,    0x2c3,  0x2c4,  0x3,    0x2,    0x2,    0x2,
      0x2c4, 0x2c5,  0x5,    0x90,   0x49,   0x2,    0x2c5,  0x2c6,  0x7,
      0x51,  0x2,    0x2,    0x2c6,  0x2c7,  0x5,    0x98,   0x4d,   0x2,
      0x2c7, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x2c8,  0x2c9,  0x7,
      0xbb,  0x2,    0x2,    0x2c9,  0x2cf,  0x7,    0x57,   0x2,    0x2,
      0x2ca, 0x2cc,  0x7,    0x8c,   0x2,    0x2,    0x2cb,  0x2cd,  0x7,
      0xc4,  0x2,    0x2,    0x2cc,  0x2cb,  0x3,    0x2,    0x2,    0x2,
      0x2cc, 0x2cd,  0x3,    0x2,    0x2,    0x2,    0x2cd,  0x2ce,  0x3,
      0x2,   0x2,    0x2,    0x2ce,  0x2d0,  0x5,    0x90,   0x49,   0x2,
      0x2cf, 0x2ca,  0x3,    0x2,    0x2,    0x2,    0x2cf,  0x2d0,  0x3,
      0x2,   0x2,    0x2,    0x2d0,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x2d1, 0x2d3,  0x7,    0x47,   0x2,    0x2,    0x2d2,  0x2d4,  0x7,
      0x11,  0x2,    0x2,    0x2d3,  0x2d2,  0x3,    0x2,    0x2,    0x2,
      0x2d3, 0x2d4,  0x3,    0x2,    0x2,    0x2,    0x2d4,  0x2d6,  0x3,
      0x2,   0x2,    0x2,    0x2d5,  0x2d7,  0x7,    0xde,   0x2,    0x2,
      0x2d6, 0x2d5,  0x3,    0x2,    0x2,    0x2,    0x2d6,  0x2d7,  0x3,
      0x2,   0x2,    0x2,    0x2d7,  0x2e3,  0x3,    0x2,    0x2,    0x2,
      0x2d8, 0x2d9,  0x7,    0x4,    0x2,    0x2,    0x2d9,  0x2de,  0x5,
      0x86,  0x44,   0x2,    0x2da,  0x2db,  0x7,    0x6,    0x2,    0x2,
      0x2db, 0x2dd,  0x5,    0x86,   0x44,   0x2,    0x2dc,  0x2da,  0x3,
      0x2,   0x2,    0x2,    0x2dd,  0x2e0,  0x3,    0x2,    0x2,    0x2,
      0x2de, 0x2dc,  0x3,    0x2,    0x2,    0x2,    0x2de,  0x2df,  0x3,
      0x2,   0x2,    0x2,    0x2df,  0x2e1,  0x3,    0x2,    0x2,    0x2,
      0x2e0, 0x2de,  0x3,    0x2,    0x2,    0x2,    0x2e1,  0x2e2,  0x7,
      0x5,   0x2,    0x2,    0x2e2,  0x2e4,  0x3,    0x2,    0x2,    0x2,
      0x2e3, 0x2d8,  0x3,    0x2,    0x2,    0x2,    0x2e3,  0x2e4,  0x3,
      0x2,   0x2,    0x2,    0x2e4,  0x2e5,  0x3,    0x2,    0x2,    0x2,
      0x2e5, 0x39f,  0x5,    0x8,    0x5,    0x2,    0x2e6,  0x2e7,  0x7,
      0xbb,  0x2,    0x2,    0x2e7,  0x2e8,  0x7,    0x28,   0x2,    0x2,
      0x2e8, 0x2e9,  0x7,    0xc4,   0x2,    0x2,    0x2e9,  0x39f,  0x5,
      0x90,  0x49,   0x2,    0x2ea,  0x2eb,  0x7,    0xbb,   0x2,    0x2,
      0x2eb, 0x2ec,  0x7,    0x28,   0x2,    0x2,    0x2ec,  0x2ed,  0x7,
      0xe0,  0x2,    0x2,    0x2ed,  0x39f,  0x5,    0x90,   0x49,   0x2,
      0x2ee, 0x2ef,  0x7,    0xbb,   0x2,    0x2,    0x2ef,  0x2f0,  0x7,
      0x28,  0x2,    0x2,    0x2f0,  0x2f1,  0x7,    0x7a,   0x2,    0x2,
      0x2f1, 0x2f2,  0x7,    0xe0,   0x2,    0x2,    0x2f2,  0x39f,  0x5,
      0x90,  0x49,   0x2,    0x2f3,  0x2f4,  0x7,    0xbb,   0x2,    0x2,
      0x2f4, 0x2f5,  0x7,    0x28,   0x2,    0x2,    0x2f5,  0x2f6,  0x7,
      0x53,  0x2,    0x2,    0x2f6,  0x2f8,  0x5,    0x90,   0x49,   0x2,
      0x2f7, 0x2f9,  0x5,    0x72,   0x3a,   0x2,    0x2f8,  0x2f7,  0x3,
      0x2,   0x2,    0x2,    0x2f8,  0x2f9,  0x3,    0x2,    0x2,    0x2,
      0x2f9, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x2fa,  0x2fb,  0x7,
      0xbb,  0x2,    0x2,    0x2fb,  0x2fe,  0x7,    0xc5,   0x2,    0x2,
      0x2fc, 0x2fd,  0x9,    0x4,    0x2,    0x2,    0x2fd,  0x2ff,  0x5,
      0x90,  0x49,   0x2,    0x2fe,  0x2fc,  0x3,    0x2,    0x2,    0x2,
      0x2fe, 0x2ff,  0x3,    0x2,    0x2,    0x2,    0x2ff,  0x306,  0x3,
      0x2,   0x2,    0x2,    0x300,  0x301,  0x7,    0x74,   0x2,    0x2,
      0x301, 0x304,  0x5,    0x60,   0x31,   0x2,    0x302,  0x303,  0x7,
      0x42,  0x2,    0x2,    0x303,  0x305,  0x5,    0x60,   0x31,   0x2,
      0x304, 0x302,  0x3,    0x2,    0x2,    0x2,    0x304,  0x305,  0x3,
      0x2,   0x2,    0x2,    0x305,  0x307,  0x3,    0x2,    0x2,    0x2,
      0x306, 0x300,  0x3,    0x2,    0x2,    0x2,    0x306,  0x307,  0x3,
      0x2,   0x2,    0x2,    0x307,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x308, 0x309,  0x7,    0xbb,   0x2,    0x2,    0x309,  0x30c,  0x7,
      0xb3,  0x2,    0x2,    0x30a,  0x30b,  0x9,    0x4,    0x2,    0x2,
      0x30b, 0x30d,  0x5,    0x9c,   0x4f,   0x2,    0x30c,  0x30a,  0x3,
      0x2,   0x2,    0x2,    0x30c,  0x30d,  0x3,    0x2,    0x2,    0x2,
      0x30d, 0x314,  0x3,    0x2,    0x2,    0x2,    0x30e,  0x30f,  0x7,
      0x74,  0x2,    0x2,    0x30f,  0x312,  0x5,    0x60,   0x31,   0x2,
      0x310, 0x311,  0x7,    0x42,   0x2,    0x2,    0x311,  0x313,  0x5,
      0x60,  0x31,   0x2,    0x312,  0x310,  0x3,    0x2,    0x2,    0x2,
      0x312, 0x313,  0x3,    0x2,    0x2,    0x2,    0x313,  0x315,  0x3,
      0x2,   0x2,    0x2,    0x314,  0x30e,  0x3,    0x2,    0x2,    0x2,
      0x314, 0x315,  0x3,    0x2,    0x2,    0x2,    0x315,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x316,  0x317,  0x7,    0xbb,   0x2,    0x2,
      0x317, 0x31e,  0x7,    0x21,   0x2,    0x2,    0x318,  0x319,  0x7,
      0x74,  0x2,    0x2,    0x319,  0x31c,  0x5,    0x60,   0x31,   0x2,
      0x31a, 0x31b,  0x7,    0x42,   0x2,    0x2,    0x31b,  0x31d,  0x5,
      0x60,  0x31,   0x2,    0x31c,  0x31a,  0x3,    0x2,    0x2,    0x2,
      0x31c, 0x31d,  0x3,    0x2,    0x2,    0x2,    0x31d,  0x31f,  0x3,
      0x2,   0x2,    0x2,    0x31e,  0x318,  0x3,    0x2,    0x2,    0x2,
      0x31e, 0x31f,  0x3,    0x2,    0x2,    0x2,    0x31f,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x320,  0x321,  0x7,    0xbb,   0x2,    0x2,
      0x321, 0x322,  0x7,    0x23,   0x2,    0x2,    0x322,  0x323,  0x9,
      0x4,   0x2,    0x2,    0x323,  0x39f,  0x5,    0x90,   0x49,   0x2,
      0x324, 0x325,  0x7,    0xbb,   0x2,    0x2,    0x325,  0x326,  0x7,
      0xbf,  0x2,    0x2,    0x326,  0x327,  0x7,    0x4f,   0x2,    0x2,
      0x327, 0x39f,  0x5,    0x90,   0x49,   0x2,    0x328,  0x329,  0x7,
      0xbb,  0x2,    0x2,    0x329,  0x32a,  0x7,    0xbf,   0x2,    0x2,
      0x32a, 0x32b,  0x7,    0x4f,   0x2,    0x2,    0x32b,  0x32c,  0x7,
      0x4,   0x2,    0x2,    0x32c,  0x32d,  0x5,    0x38,   0x1d,   0x2,
      0x32d, 0x32e,  0x7,    0x5,    0x2,    0x2,    0x32e,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x32f,  0x331,  0x7,    0xbb,   0x2,    0x2,
      0x330, 0x332,  0x7,    0x2b,   0x2,    0x2,    0x331,  0x330,  0x3,
      0x2,   0x2,    0x2,    0x331,  0x332,  0x3,    0x2,    0x2,    0x2,
      0x332, 0x333,  0x3,    0x2,    0x2,    0x2,    0x333,  0x336,  0x7,
      0xad,  0x2,    0x2,    0x334,  0x335,  0x9,    0x4,    0x2,    0x2,
      0x335, 0x337,  0x5,    0x9c,   0x4f,   0x2,    0x336,  0x334,  0x3,
      0x2,   0x2,    0x2,    0x336,  0x337,  0x3,    0x2,    0x2,    0x2,
      0x337, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x338,  0x339,  0x7,
      0xbb,  0x2,    0x2,    0x339,  0x33a,  0x7,    0xac,   0x2,    0x2,
      0x33a, 0x33d,  0x7,    0x57,   0x2,    0x2,    0x33b,  0x33c,  0x9,
      0x4,   0x2,    0x2,    0x33c,  0x33e,  0x5,    0x9c,   0x4f,   0x2,
      0x33d, 0x33b,  0x3,    0x2,    0x2,    0x2,    0x33d,  0x33e,  0x3,
      0x2,   0x2,    0x2,    0x33e,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x33f, 0x340,  0x7,    0x38,   0x2,    0x2,    0x340,  0x39f,  0x5,
      0x90,  0x49,   0x2,    0x341,  0x342,  0x7,    0x37,   0x2,    0x2,
      0x342, 0x39f,  0x5,    0x90,   0x49,   0x2,    0x343,  0x344,  0x7,
      0xbb,  0x2,    0x2,    0x344,  0x34b,  0x7,    0x54,   0x2,    0x2,
      0x345, 0x346,  0x7,    0x74,   0x2,    0x2,    0x346,  0x349,  0x5,
      0x60,  0x31,   0x2,    0x347,  0x348,  0x7,    0x42,   0x2,    0x2,
      0x348, 0x34a,  0x5,    0x60,   0x31,   0x2,    0x349,  0x347,  0x3,
      0x2,   0x2,    0x2,    0x349,  0x34a,  0x3,    0x2,    0x2,    0x2,
      0x34a, 0x34c,  0x3,    0x2,    0x2,    0x2,    0x34b,  0x345,  0x3,
      0x2,   0x2,    0x2,    0x34b,  0x34c,  0x3,    0x2,    0x2,    0x2,
      0x34c, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x34d,  0x34e,  0x7,
      0xbb,  0x2,    0x2,    0x34e,  0x355,  0x7,    0xb8,   0x2,    0x2,
      0x34f, 0x350,  0x7,    0x74,   0x2,    0x2,    0x350,  0x353,  0x5,
      0x60,  0x31,   0x2,    0x351,  0x352,  0x7,    0x42,   0x2,    0x2,
      0x352, 0x354,  0x5,    0x60,   0x31,   0x2,    0x353,  0x351,  0x3,
      0x2,   0x2,    0x2,    0x353,  0x354,  0x3,    0x2,    0x2,    0x2,
      0x354, 0x356,  0x3,    0x2,    0x2,    0x2,    0x355,  0x34f,  0x3,
      0x2,   0x2,    0x2,    0x355,  0x356,  0x3,    0x2,    0x2,    0x2,
      0x356, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x357,  0x358,  0x7,
      0xb9,  0x2,    0x2,    0x358,  0x359,  0x7,    0xb8,   0x2,    0x2,
      0x359, 0x35a,  0x5,    0x90,   0x49,   0x2,    0x35a,  0x35b,  0x7,
      0xe8,  0x2,    0x2,    0x35b,  0x35c,  0x5,    0x56,   0x2c,   0x2,
      0x35c, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x35d,  0x35e,  0x7,
      0xa5,  0x2,    0x2,    0x35e,  0x35f,  0x7,    0xb8,   0x2,    0x2,
      0x35f, 0x39f,  0x5,    0x90,   0x49,   0x2,    0x360,  0x361,  0x7,
      0xbe,  0x2,    0x2,    0x361,  0x36a,  0x7,    0xcd,   0x2,    0x2,
      0x362, 0x367,  0x5,    0x88,   0x45,   0x2,    0x363,  0x364,  0x7,
      0x6,   0x2,    0x2,    0x364,  0x366,  0x5,    0x88,   0x45,   0x2,
      0x365, 0x363,  0x3,    0x2,    0x2,    0x2,    0x366,  0x369,  0x3,
      0x2,   0x2,    0x2,    0x367,  0x365,  0x3,    0x2,    0x2,    0x2,
      0x367, 0x368,  0x3,    0x2,    0x2,    0x2,    0x368,  0x36b,  0x3,
      0x2,   0x2,    0x2,    0x369,  0x367,  0x3,    0x2,    0x2,    0x2,
      0x36a, 0x362,  0x3,    0x2,    0x2,    0x2,    0x36a,  0x36b,  0x3,
      0x2,   0x2,    0x2,    0x36b,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x36c, 0x36e,  0x7,    0x25,   0x2,    0x2,    0x36d,  0x36f,  0x7,
      0xe4,  0x2,    0x2,    0x36e,  0x36d,  0x3,    0x2,    0x2,    0x2,
      0x36e, 0x36f,  0x3,    0x2,    0x2,    0x2,    0x36f,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x370,  0x372,  0x7,    0xae,   0x2,    0x2,
      0x371, 0x373,  0x7,    0xe4,   0x2,    0x2,    0x372,  0x371,  0x3,
      0x2,   0x2,    0x2,    0x372,  0x373,  0x3,    0x2,    0x2,    0x2,
      0x373, 0x39f,  0x3,    0x2,    0x2,    0x2,    0x374,  0x375,  0x7,
      0x99,  0x2,    0x2,    0x375,  0x376,  0x5,    0x9c,   0x4f,   0x2,
      0x376, 0x377,  0x7,    0x51,   0x2,    0x2,    0x377,  0x378,  0x5,
      0x8,   0x5,    0x2,    0x378,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x379, 0x37a,  0x7,    0x34,   0x2,    0x2,    0x37a,  0x37b,  0x7,
      0x99,  0x2,    0x2,    0x37b,  0x39f,  0x5,    0x9c,   0x4f,   0x2,
      0x37c, 0x37d,  0x7,    0x45,   0x2,    0x2,    0x37d,  0x387,  0x5,
      0x9c,  0x4f,   0x2,    0x37e,  0x37f,  0x7,    0xdb,   0x2,    0x2,
      0x37f, 0x384,  0x5,    0x56,   0x2c,   0x2,    0x380,  0x381,  0x7,
      0x6,   0x2,    0x2,    0x381,  0x383,  0x5,    0x56,   0x2c,   0x2,
      0x382, 0x380,  0x3,    0x2,    0x2,    0x2,    0x383,  0x386,  0x3,
      0x2,   0x2,    0x2,    0x384,  0x382,  0x3,    0x2,    0x2,    0x2,
      0x384, 0x385,  0x3,    0x2,    0x2,    0x2,    0x385,  0x388,  0x3,
      0x2,   0x2,    0x2,    0x386,  0x384,  0x3,    0x2,    0x2,    0x2,
      0x387, 0x37e,  0x3,    0x2,    0x2,    0x2,    0x387,  0x388,  0x3,
      0x2,   0x2,    0x2,    0x388,  0x39f,  0x3,    0x2,    0x2,    0x2,
      0x389, 0x38a,  0x7,    0x38,   0x2,    0x2,    0x38a,  0x38b,  0x7,
      0x63,  0x2,    0x2,    0x38b,  0x39f,  0x5,    0x9c,   0x4f,   0x2,
      0x38c, 0x38d,  0x7,    0x38,   0x2,    0x2,    0x38d,  0x38e,  0x7,
      0x93,  0x2,    0x2,    0x38e,  0x39f,  0x5,    0x9c,   0x4f,   0x2,
      0x38f, 0x390,  0x7,    0xd8,   0x2,    0x2,    0x390,  0x391,  0x5,
      0x90,  0x49,   0x2,    0x391,  0x392,  0x7,    0xb9,   0x2,    0x2,
      0x392, 0x397,  0x5,    0x84,   0x43,   0x2,    0x393,  0x394,  0x7,
      0x6,   0x2,    0x2,    0x394,  0x396,  0x5,    0x84,   0x43,   0x2,
      0x395, 0x393,  0x3,    0x2,    0x2,    0x2,    0x396,  0x399,  0x3,
      0x2,   0x2,    0x2,    0x397,  0x395,  0x3,    0x2,    0x2,    0x2,
      0x397, 0x398,  0x3,    0x2,    0x2,    0x2,    0x398,  0x39c,  0x3,
      0x2,   0x2,    0x2,    0x399,  0x397,  0x3,    0x2,    0x2,    0x2,
      0x39a, 0x39b,  0x7,    0xe2,   0x2,    0x2,    0x39b,  0x39d,  0x5,
      0x58,  0x2d,   0x2,    0x39c,  0x39a,  0x3,    0x2,    0x2,    0x2,
      0x39c, 0x39d,  0x3,    0x2,    0x2,    0x2,    0x39d,  0x39f,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0xbd,   0x3,    0x2,    0x2,    0x2,
      0x39e, 0xbe,   0x3,    0x2,    0x2,    0x2,    0x39e,  0xc0,   0x3,
      0x2,   0x2,    0x2,    0x39e,  0xc5,   0x3,    0x2,    0x2,    0x2,
      0x39e, 0xd1,   0x3,    0x2,    0x2,    0x2,    0x39e,  0xdb,   0x3,
      0x2,   0x2,    0x2,    0x39e,  0xe2,   0x3,    0x2,    0x2,    0x2,
      0x39e, 0x104,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x11e,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x125,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x12d,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x134,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x137,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x142,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x153,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x162,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x172,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x17c,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x18b,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x19b,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x1ab,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x1b6,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x1bc,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x1ce,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x1dc,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x1e7,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x1ee,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x207,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x20f,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x216,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x235,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x23d,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x24a,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x259,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x261,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x264,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x279,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x28e,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x295,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x2ae,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x2c8,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x2d1,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x2e6,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x2ea,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x2ee,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x2f3,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x2fa,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x308,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x316,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x320,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x324,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x328,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x32f,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x338,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x33f,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x341,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x343,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x34d,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x357,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x35d,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x360,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x36c,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x370,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x374,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x379,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x37c,  0x3,
      0x2,   0x2,    0x2,    0x39e,  0x389,  0x3,    0x2,    0x2,    0x2,
      0x39e, 0x38c,  0x3,    0x2,    0x2,    0x2,    0x39e,  0x38f,  0x3,
      0x2,   0x2,    0x2,    0x39f,  0x9,    0x3,    0x2,    0x2,    0x2,
      0x3a0, 0x3a2,  0x5,    0xc,    0x7,    0x2,    0x3a1,  0x3a0,  0x3,
      0x2,   0x2,    0x2,    0x3a1,  0x3a2,  0x3,    0x2,    0x2,    0x2,
      0x3a2, 0x3a3,  0x3,    0x2,    0x2,    0x2,    0x3a3,  0x3a4,  0x5,
      0x30,  0x19,   0x2,    0x3a4,  0xb,    0x3,    0x2,    0x2,    0x2,
      0x3a5, 0x3a7,  0x7,    0xe3,   0x2,    0x2,    0x3a6,  0x3a8,  0x7,
      0x9f,  0x2,    0x2,    0x3a7,  0x3a6,  0x3,    0x2,    0x2,    0x2,
      0x3a7, 0x3a8,  0x3,    0x2,    0x2,    0x2,    0x3a8,  0x3a9,  0x3,
      0x2,   0x2,    0x2,    0x3a9,  0x3ae,  0x5,    0x40,   0x21,   0x2,
      0x3aa, 0x3ab,  0x7,    0x6,    0x2,    0x2,    0x3ab,  0x3ad,  0x5,
      0x40,  0x21,   0x2,    0x3ac,  0x3aa,  0x3,    0x2,    0x2,    0x2,
      0x3ad, 0x3b0,  0x3,    0x2,    0x2,    0x2,    0x3ae,  0x3ac,  0x3,
      0x2,   0x2,    0x2,    0x3ae,  0x3af,  0x3,    0x2,    0x2,    0x2,
      0x3af, 0xd,    0x3,    0x2,    0x2,    0x2,    0x3b0,  0x3ae,  0x3,
      0x2,   0x2,    0x2,    0x3b1,  0x3b5,  0x5,    0xa0,   0x51,   0x2,
      0x3b2, 0x3b5,  0x5,    0x10,   0x9,    0x2,    0x3b3,  0x3b5,  0x5,
      0x12,  0xa,    0x2,    0x3b4,  0x3b1,  0x3,    0x2,    0x2,    0x2,
      0x3b4, 0x3b2,  0x3,    0x2,    0x2,    0x2,    0x3b4,  0x3b3,  0x3,
      0x2,   0x2,    0x2,    0x3b5,  0xf,    0x3,    0x2,    0x2,    0x2,
      0x3b6, 0x3b7,  0x5,    0x9c,   0x4f,   0x2,    0x3b7,  0x3ba,  0x5,
      0x74,  0x3b,   0x2,    0x3b8,  0x3b9,  0x7,    0x86,   0x2,    0x2,
      0x3b9, 0x3bb,  0x7,    0x87,   0x2,    0x2,    0x3ba,  0x3b8,  0x3,
      0x2,   0x2,    0x2,    0x3ba,  0x3bb,  0x3,    0x2,    0x2,    0x2,
      0x3bb, 0x3be,  0x3,    0x2,    0x2,    0x2,    0x3bc,  0x3bd,  0x7,
      0x24,  0x2,    0x2,    0x3bd,  0x3bf,  0x5,    0x60,   0x31,   0x2,
      0x3be, 0x3bc,  0x3,    0x2,    0x2,    0x2,    0x3be,  0x3bf,  0x3,
      0x2,   0x2,    0x2,    0x3bf,  0x3c2,  0x3,    0x2,    0x2,    0x2,
      0x3c0, 0x3c1,  0x7,    0xe3,   0x2,    0x2,    0x3c1,  0x3c3,  0x5,
      0x14,  0xb,    0x2,    0x3c2,  0x3c0,  0x3,    0x2,    0x2,    0x2,
      0x3c2, 0x3c3,  0x3,    0x2,    0x2,    0x2,    0x3c3,  0x11,   0x3,
      0x2,   0x2,    0x2,    0x3c4,  0x3c5,  0x7,    0x74,   0x2,    0x2,
      0x3c5, 0x3c8,  0x5,    0x90,   0x49,   0x2,    0x3c6,  0x3c7,  0x9,
      0x5,   0x2,    0x2,    0x3c7,  0x3c9,  0x7,    0x9c,   0x2,    0x2,
      0x3c8, 0x3c6,  0x3,    0x2,    0x2,    0x2,    0x3c8,  0x3c9,  0x3,
      0x2,   0x2,    0x2,    0x3c9,  0x13,   0x3,    0x2,    0x2,    0x2,
      0x3ca, 0x3cb,  0x7,    0x4,    0x2,    0x2,    0x3cb,  0x3d0,  0x5,
      0x16,  0xc,    0x2,    0x3cc,  0x3cd,  0x7,    0x6,    0x2,    0x2,
      0x3cd, 0x3cf,  0x5,    0x16,   0xc,    0x2,    0x3ce,  0x3cc,  0x3,
      0x2,   0x2,    0x2,    0x3cf,  0x3d2,  0x3,    0x2,    0x2,    0x2,
      0x3d0, 0x3ce,  0x3,    0x2,    0x2,    0x2,    0x3d0,  0x3d1,  0x3,
      0x2,   0x2,    0x2,    0x3d1,  0x3d3,  0x3,    0x2,    0x2,    0x2,
      0x3d2, 0x3d0,  0x3,    0x2,    0x2,    0x2,    0x3d3,  0x3d4,  0x7,
      0x5,   0x2,    0x2,    0x3d4,  0x15,   0x3,    0x2,    0x2,    0x2,
      0x3d5, 0x3d6,  0x5,    0x9c,   0x4f,   0x2,    0x3d6,  0x3d7,  0x7,
      0xe8,  0x2,    0x2,    0x3d7,  0x3d8,  0x5,    0x56,   0x2c,   0x2,
      0x3d8, 0x17,   0x3,    0x2,    0x2,    0x2,    0x3d9,  0x3da,  0x5,
      0x9c,  0x4f,   0x2,    0x3da,  0x3db,  0x5,    0x74,   0x3b,   0x2,
      0x3db, 0x19,   0x3,    0x2,    0x2,    0x2,    0x3dc,  0x3de,  0x5,
      0x1c,  0xf,    0x2,    0x3dd,  0x3dc,  0x3,    0x2,    0x2,    0x2,
      0x3de, 0x3e1,  0x3,    0x2,    0x2,    0x2,    0x3df,  0x3dd,  0x3,
      0x2,   0x2,    0x2,    0x3df,  0x3e0,  0x3,    0x2,    0x2,    0x2,
      0x3e0, 0x1b,   0x3,    0x2,    0x2,    0x2,    0x3e1,  0x3df,  0x3,
      0x2,   0x2,    0x2,    0x3e2,  0x3e3,  0x7,    0x6f,   0x2,    0x2,
      0x3e3, 0x3e7,  0x5,    0x28,   0x15,   0x2,    0x3e4,  0x3e7,  0x5,
      0x2a,  0x16,   0x2,    0x3e5,  0x3e7,  0x5,    0x2c,   0x17,   0x2,
      0x3e6, 0x3e2,  0x3,    0x2,    0x2,    0x2,    0x3e6,  0x3e4,  0x3,
      0x2,   0x2,    0x2,    0x3e6,  0x3e5,  0x3,    0x2,    0x2,    0x2,
      0x3e7, 0x1d,   0x3,    0x2,    0x2,    0x2,    0x3e8,  0x3ea,  0x5,
      0x20,  0x11,   0x2,    0x3e9,  0x3e8,  0x3,    0x2,    0x2,    0x2,
      0x3ea, 0x3ed,  0x3,    0x2,    0x2,    0x2,    0x3eb,  0x3e9,  0x3,
      0x2,   0x2,    0x2,    0x3eb,  0x3ec,  0x3,    0x2,    0x2,    0x2,
      0x3ec, 0x1f,   0x3,    0x2,    0x2,    0x2,    0x3ed,  0x3eb,  0x3,
      0x2,   0x2,    0x2,    0x3ee,  0x3ef,  0x5,    0x2c,   0x17,   0x2,
      0x3ef, 0x21,   0x3,    0x2,    0x2,    0x2,    0x3f0,  0x3f3,  0x5,
      0x24,  0x13,   0x2,    0x3f1,  0x3f3,  0x5,    0x26,   0x14,   0x2,
      0x3f2, 0x3f0,  0x3,    0x2,    0x2,    0x2,    0x3f2,  0x3f1,  0x3,
      0x2,   0x2,    0x2,    0x3f3,  0x23,   0x3,    0x2,    0x2,    0x2,
      0x3f4, 0x3f5,  0x7,    0xa8,   0x2,    0x2,    0x3f5,  0x3f6,  0x5,
      0x56,  0x2c,   0x2,    0x3f6,  0x25,   0x3,    0x2,    0x2,    0x2,
      0x3f7, 0x3fa,  0x7,    0x49,   0x2,    0x2,    0x3f8,  0x3f9,  0x7,
      0x7d,  0x2,    0x2,    0x3f9,  0x3fb,  0x5,    0x2e,   0x18,   0x2,
      0x3fa, 0x3f8,  0x3,    0x2,    0x2,    0x2,    0x3fa,  0x3fb,  0x3,
      0x2,   0x2,    0x2,    0x3fb,  0x27,   0x3,    0x2,    0x2,    0x2,
      0x3fc, 0x3ff,  0x7,    0xbd,   0x2,    0x2,    0x3fd,  0x3ff,  0x5,
      0x9c,  0x4f,   0x2,    0x3fe,  0x3fc,  0x3,    0x2,    0x2,    0x2,
      0x3fe, 0x3fd,  0x3,    0x2,    0x2,    0x2,    0x3ff,  0x29,   0x3,
      0x2,   0x2,    0x2,    0x400,  0x404,  0x7,    0x39,   0x2,    0x2,
      0x401, 0x402,  0x7,    0x86,   0x2,    0x2,    0x402,  0x404,  0x7,
      0x39,  0x2,    0x2,    0x403,  0x400,  0x3,    0x2,    0x2,    0x2,
      0x403, 0x401,  0x3,    0x2,    0x2,    0x2,    0x404,  0x2b,   0x3,
      0x2,   0x2,    0x2,    0x405,  0x406,  0x7,    0xa9,   0x2,    0x2,
      0x406, 0x407,  0x7,    0x87,   0x2,    0x2,    0x407,  0x408,  0x7,
      0x8c,  0x2,    0x2,    0x408,  0x409,  0x7,    0x87,   0x2,    0x2,
      0x409, 0x40f,  0x7,    0x63,   0x2,    0x2,    0x40a,  0x40b,  0x7,
      0x1d,  0x2,    0x2,    0x40b,  0x40c,  0x7,    0x8c,   0x2,    0x2,
      0x40c, 0x40d,  0x7,    0x87,   0x2,    0x2,    0x40d,  0x40f,  0x7,
      0x63,  0x2,    0x2,    0x40e,  0x405,  0x3,    0x2,    0x2,    0x2,
      0x40e, 0x40a,  0x3,    0x2,    0x2,    0x2,    0x40f,  0x2d,   0x3,
      0x2,   0x2,    0x2,    0x410,  0x411,  0x5,    0x9c,   0x4f,   0x2,
      0x411, 0x2f,   0x3,    0x2,    0x2,    0x2,    0x412,  0x41d,  0x5,
      0x32,  0x1a,   0x2,    0x413,  0x414,  0x7,    0x90,   0x2,    0x2,
      0x414, 0x415,  0x7,    0x1b,   0x2,    0x2,    0x415,  0x41a,  0x5,
      0x36,  0x1c,   0x2,    0x416,  0x417,  0x7,    0x6,    0x2,    0x2,
      0x417, 0x419,  0x5,    0x36,   0x1c,   0x2,    0x418,  0x416,  0x3,
      0x2,   0x2,    0x2,    0x419,  0x41c,  0x3,    0x2,    0x2,    0x2,
      0x41a, 0x418,  0x3,    0x2,    0x2,    0x2,    0x41a,  0x41b,  0x3,
      0x2,   0x2,    0x2,    0x41b,  0x41e,  0x3,    0x2,    0x2,    0x2,
      0x41c, 0x41a,  0x3,    0x2,    0x2,    0x2,    0x41d,  0x413,  0x3,
      0x2,   0x2,    0x2,    0x41d,  0x41e,  0x3,    0x2,    0x2,    0x2,
      0x41e, 0x424,  0x3,    0x2,    0x2,    0x2,    0x41f,  0x420,  0x7,
      0x8b,  0x2,    0x2,    0x420,  0x422,  0x7,    0xf7,   0x2,    0x2,
      0x421, 0x423,  0x9,    0x6,    0x2,    0x2,    0x422,  0x421,  0x3,
      0x2,   0x2,    0x2,    0x422,  0x423,  0x3,    0x2,    0x2,    0x2,
      0x423, 0x425,  0x3,    0x2,    0x2,    0x2,    0x424,  0x41f,  0x3,
      0x2,   0x2,    0x2,    0x424,  0x425,  0x3,    0x2,    0x2,    0x2,
      0x425, 0x42f,  0x3,    0x2,    0x2,    0x2,    0x426,  0x427,  0x7,
      0x75,  0x2,    0x2,    0x427,  0x42e,  0x9,    0x7,    0x2,    0x2,
      0x428, 0x429,  0x7,    0x4b,   0x2,    0x2,    0x429,  0x42a,  0x7,
      0x4d,  0x2,    0x2,    0x42a,  0x42b,  0x7,    0xf7,   0x2,    0x2,
      0x42b, 0x42c,  0x7,    0xb1,   0x2,    0x2,    0x42c,  0x42e,  0x7,
      0x8d,  0x2,    0x2,    0x42d,  0x426,  0x3,    0x2,    0x2,    0x2,
      0x42d, 0x428,  0x3,    0x2,    0x2,    0x2,    0x42e,  0x430,  0x3,
      0x2,   0x2,    0x2,    0x42f,  0x42d,  0x3,    0x2,    0x2,    0x2,
      0x42f, 0x430,  0x3,    0x2,    0x2,    0x2,    0x430,  0x31,   0x3,
      0x2,   0x2,    0x2,    0x431,  0x432,  0x8,    0x1a,   0x1,    0x2,
      0x432, 0x433,  0x5,    0x34,   0x1b,   0x2,    0x433,  0x442,  0x3,
      0x2,   0x2,    0x2,    0x434,  0x435,  0xc,    0x4,    0x2,    0x2,
      0x435, 0x437,  0x7,    0x65,   0x2,    0x2,    0x436,  0x438,  0x5,
      0x42,  0x22,   0x2,    0x437,  0x436,  0x3,    0x2,    0x2,    0x2,
      0x437, 0x438,  0x3,    0x2,    0x2,    0x2,    0x438,  0x439,  0x3,
      0x2,   0x2,    0x2,    0x439,  0x441,  0x5,    0x32,   0x1a,   0x5,
      0x43a, 0x43b,  0xc,    0x3,    0x2,    0x2,    0x43b,  0x43d,  0x9,
      0x8,   0x2,    0x2,    0x43c,  0x43e,  0x5,    0x42,   0x22,   0x2,
      0x43d, 0x43c,  0x3,    0x2,    0x2,    0x2,    0x43d,  0x43e,  0x3,
      0x2,   0x2,    0x2,    0x43e,  0x43f,  0x3,    0x2,    0x2,    0x2,
      0x43f, 0x441,  0x5,    0x32,   0x1a,   0x4,    0x440,  0x434,  0x3,
      0x2,   0x2,    0x2,    0x440,  0x43a,  0x3,    0x2,    0x2,    0x2,
      0x441, 0x444,  0x3,    0x2,    0x2,    0x2,    0x442,  0x440,  0x3,
      0x2,   0x2,    0x2,    0x442,  0x443,  0x3,    0x2,    0x2,    0x2,
      0x443, 0x33,   0x3,    0x2,    0x2,    0x2,    0x444,  0x442,  0x3,
      0x2,   0x2,    0x2,    0x445,  0x456,  0x5,    0x38,   0x1d,   0x2,
      0x446, 0x447,  0x7,    0xc4,   0x2,    0x2,    0x447,  0x456,  0x5,
      0x90,  0x49,   0x2,    0x448,  0x449,  0x7,    0xdd,   0x2,    0x2,
      0x449, 0x44e,  0x5,    0x56,   0x2c,   0x2,    0x44a,  0x44b,  0x7,
      0x6,   0x2,    0x2,    0x44b,  0x44d,  0x5,    0x56,   0x2c,   0x2,
      0x44c, 0x44a,  0x3,    0x2,    0x2,    0x2,    0x44d,  0x450,  0x3,
      0x2,   0x2,    0x2,    0x44e,  0x44c,  0x3,    0x2,    0x2,    0x2,
      0x44e, 0x44f,  0x3,    0x2,    0x2,    0x2,    0x44f,  0x456,  0x3,
      0x2,   0x2,    0x2,    0x450,  0x44e,  0x3,    0x2,    0x2,    0x2,
      0x451, 0x452,  0x7,    0x4,    0x2,    0x2,    0x452,  0x453,  0x5,
      0x30,  0x19,   0x2,    0x453,  0x454,  0x7,    0x5,    0x2,    0x2,
      0x454, 0x456,  0x3,    0x2,    0x2,    0x2,    0x455,  0x445,  0x3,
      0x2,   0x2,    0x2,    0x455,  0x446,  0x3,    0x2,    0x2,    0x2,
      0x455, 0x448,  0x3,    0x2,    0x2,    0x2,    0x455,  0x451,  0x3,
      0x2,   0x2,    0x2,    0x456,  0x35,   0x3,    0x2,    0x2,    0x2,
      0x457, 0x459,  0x5,    0x56,   0x2c,   0x2,    0x458,  0x45a,  0x9,
      0x9,   0x2,    0x2,    0x459,  0x458,  0x3,    0x2,    0x2,    0x2,
      0x459, 0x45a,  0x3,    0x2,    0x2,    0x2,    0x45a,  0x45d,  0x3,
      0x2,   0x2,    0x2,    0x45b,  0x45c,  0x7,    0x89,   0x2,    0x2,
      0x45c, 0x45e,  0x9,    0xa,    0x2,    0x2,    0x45d,  0x45b,  0x3,
      0x2,   0x2,    0x2,    0x45d,  0x45e,  0x3,    0x2,    0x2,    0x2,
      0x45e, 0x37,   0x3,    0x2,    0x2,    0x2,    0x45f,  0x461,  0x7,
      0xb6,  0x2,    0x2,    0x460,  0x462,  0x5,    0x42,   0x22,   0x2,
      0x461, 0x460,  0x3,    0x2,    0x2,    0x2,    0x461,  0x462,  0x3,
      0x2,   0x2,    0x2,    0x462,  0x463,  0x3,    0x2,    0x2,    0x2,
      0x463, 0x468,  0x5,    0x44,   0x23,   0x2,    0x464,  0x465,  0x7,
      0x6,   0x2,    0x2,    0x465,  0x467,  0x5,    0x44,   0x23,   0x2,
      0x466, 0x464,  0x3,    0x2,    0x2,    0x2,    0x467,  0x46a,  0x3,
      0x2,   0x2,    0x2,    0x468,  0x466,  0x3,    0x2,    0x2,    0x2,
      0x468, 0x469,  0x3,    0x2,    0x2,    0x2,    0x469,  0x474,  0x3,
      0x2,   0x2,    0x2,    0x46a,  0x468,  0x3,    0x2,    0x2,    0x2,
      0x46b, 0x46c,  0x7,    0x51,   0x2,    0x2,    0x46c,  0x471,  0x5,
      0x46,  0x24,   0x2,    0x46d,  0x46e,  0x7,    0x6,    0x2,    0x2,
      0x46e, 0x470,  0x5,    0x46,   0x24,   0x2,    0x46f,  0x46d,  0x3,
      0x2,   0x2,    0x2,    0x470,  0x473,  0x3,    0x2,    0x2,    0x2,
      0x471, 0x46f,  0x3,    0x2,    0x2,    0x2,    0x471,  0x472,  0x3,
      0x2,   0x2,    0x2,    0x472,  0x475,  0x3,    0x2,    0x2,    0x2,
      0x473, 0x471,  0x3,    0x2,    0x2,    0x2,    0x474,  0x46b,  0x3,
      0x2,   0x2,    0x2,    0x474,  0x475,  0x3,    0x2,    0x2,    0x2,
      0x475, 0x478,  0x3,    0x2,    0x2,    0x2,    0x476,  0x477,  0x7,
      0xe2,  0x2,    0x2,    0x477,  0x479,  0x5,    0x58,   0x2d,   0x2,
      0x478, 0x476,  0x3,    0x2,    0x2,    0x2,    0x478,  0x479,  0x3,
      0x2,   0x2,    0x2,    0x479,  0x47d,  0x3,    0x2,    0x2,    0x2,
      0x47a, 0x47b,  0x7,    0x59,   0x2,    0x2,    0x47b,  0x47c,  0x7,
      0x1b,  0x2,    0x2,    0x47c,  0x47e,  0x5,    0x3a,   0x1e,   0x2,
      0x47d, 0x47a,  0x3,    0x2,    0x2,    0x2,    0x47d,  0x47e,  0x3,
      0x2,   0x2,    0x2,    0x47e,  0x481,  0x3,    0x2,    0x2,    0x2,
      0x47f, 0x480,  0x7,    0x5c,   0x2,    0x2,    0x480,  0x482,  0x5,
      0x58,  0x2d,   0x2,    0x481,  0x47f,  0x3,    0x2,    0x2,    0x2,
      0x481, 0x482,  0x3,    0x2,    0x2,    0x2,    0x482,  0x39,   0x3,
      0x2,   0x2,    0x2,    0x483,  0x485,  0x5,    0x42,   0x22,   0x2,
      0x484, 0x483,  0x3,    0x2,    0x2,    0x2,    0x484,  0x485,  0x3,
      0x2,   0x2,    0x2,    0x485,  0x486,  0x3,    0x2,    0x2,    0x2,
      0x486, 0x48b,  0x5,    0x3c,   0x1f,   0x2,    0x487,  0x488,  0x7,
      0x6,   0x2,    0x2,    0x488,  0x48a,  0x5,    0x3c,   0x1f,   0x2,
      0x489, 0x487,  0x3,    0x2,    0x2,    0x2,    0x48a,  0x48d,  0x3,
      0x2,   0x2,    0x2,    0x48b,  0x489,  0x3,    0x2,    0x2,    0x2,
      0x48b, 0x48c,  0x3,    0x2,    0x2,    0x2,    0x48c,  0x3b,   0x3,
      0x2,   0x2,    0x2,    0x48d,  0x48b,  0x3,    0x2,    0x2,    0x2,
      0x48e, 0x4b7,  0x5,    0x3e,   0x20,   0x2,    0x48f,  0x490,  0x7,
      0xaf,  0x2,    0x2,    0x490,  0x499,  0x7,    0x4,    0x2,    0x2,
      0x491, 0x496,  0x5,    0x56,   0x2c,   0x2,    0x492,  0x493,  0x7,
      0x6,   0x2,    0x2,    0x493,  0x495,  0x5,    0x56,   0x2c,   0x2,
      0x494, 0x492,  0x3,    0x2,    0x2,    0x2,    0x495,  0x498,  0x3,
      0x2,   0x2,    0x2,    0x496,  0x494,  0x3,    0x2,    0x2,    0x2,
      0x496, 0x497,  0x3,    0x2,    0x2,    0x2,    0x497,  0x49a,  0x3,
      0x2,   0x2,    0x2,    0x498,  0x496,  0x3,    0x2,    0x2,    0x2,
      0x499, 0x491,  0x3,    0x2,    0x2,    0x2,    0x499,  0x49a,  0x3,
      0x2,   0x2,    0x2,    0x49a,  0x49b,  0x3,    0x2,    0x2,    0x2,
      0x49b, 0x4b7,  0x7,    0x5,    0x2,    0x2,    0x49c,  0x49d,  0x7,
      0x2a,  0x2,    0x2,    0x49d,  0x4a6,  0x7,    0x4,    0x2,    0x2,
      0x49e, 0x4a3,  0x5,    0x56,   0x2c,   0x2,    0x49f,  0x4a0,  0x7,
      0x6,   0x2,    0x2,    0x4a0,  0x4a2,  0x5,    0x56,   0x2c,   0x2,
      0x4a1, 0x49f,  0x3,    0x2,    0x2,    0x2,    0x4a2,  0x4a5,  0x3,
      0x2,   0x2,    0x2,    0x4a3,  0x4a1,  0x3,    0x2,    0x2,    0x2,
      0x4a3, 0x4a4,  0x3,    0x2,    0x2,    0x2,    0x4a4,  0x4a7,  0x3,
      0x2,   0x2,    0x2,    0x4a5,  0x4a3,  0x3,    0x2,    0x2,    0x2,
      0x4a6, 0x49e,  0x3,    0x2,    0x2,    0x2,    0x4a6,  0x4a7,  0x3,
      0x2,   0x2,    0x2,    0x4a7,  0x4a8,  0x3,    0x2,    0x2,    0x2,
      0x4a8, 0x4b7,  0x7,    0x5,    0x2,    0x2,    0x4a9,  0x4aa,  0x7,
      0x5a,  0x2,    0x2,    0x4aa,  0x4ab,  0x7,    0xba,   0x2,    0x2,
      0x4ab, 0x4ac,  0x7,    0x4,    0x2,    0x2,    0x4ac,  0x4b1,  0x5,
      0x3e,  0x20,   0x2,    0x4ad,  0x4ae,  0x7,    0x6,    0x2,    0x2,
      0x4ae, 0x4b0,  0x5,    0x3e,   0x20,   0x2,    0x4af,  0x4ad,  0x3,
      0x2,   0x2,    0x2,    0x4b0,  0x4b3,  0x3,    0x2,    0x2,    0x2,
      0x4b1, 0x4af,  0x3,    0x2,    0x2,    0x2,    0x4b1,  0x4b2,  0x3,
      0x2,   0x2,    0x2,    0x4b2,  0x4b4,  0x3,    0x2,    0x2,    0x2,
      0x4b3, 0x4b1,  0x3,    0x2,    0x2,    0x2,    0x4b4,  0x4b5,  0x7,
      0x5,   0x2,    0x2,    0x4b5,  0x4b7,  0x3,    0x2,    0x2,    0x2,
      0x4b6, 0x48e,  0x3,    0x2,    0x2,    0x2,    0x4b6,  0x48f,  0x3,
      0x2,   0x2,    0x2,    0x4b6,  0x49c,  0x3,    0x2,    0x2,    0x2,
      0x4b6, 0x4a9,  0x3,    0x2,    0x2,    0x2,    0x4b7,  0x3d,   0x3,
      0x2,   0x2,    0x2,    0x4b8,  0x4c1,  0x7,    0x4,    0x2,    0x2,
      0x4b9, 0x4be,  0x5,    0x56,   0x2c,   0x2,    0x4ba,  0x4bb,  0x7,
      0x6,   0x2,    0x2,    0x4bb,  0x4bd,  0x5,    0x56,   0x2c,   0x2,
      0x4bc, 0x4ba,  0x3,    0x2,    0x2,    0x2,    0x4bd,  0x4c0,  0x3,
      0x2,   0x2,    0x2,    0x4be,  0x4bc,  0x3,    0x2,    0x2,    0x2,
      0x4be, 0x4bf,  0x3,    0x2,    0x2,    0x2,    0x4bf,  0x4c2,  0x3,
      0x2,   0x2,    0x2,    0x4c0,  0x4be,  0x3,    0x2,    0x2,    0x2,
      0x4c1, 0x4b9,  0x3,    0x2,    0x2,    0x2,    0x4c1,  0x4c2,  0x3,
      0x2,   0x2,    0x2,    0x4c2,  0x4c3,  0x3,    0x2,    0x2,    0x2,
      0x4c3, 0x4c6,  0x7,    0x5,    0x2,    0x2,    0x4c4,  0x4c6,  0x5,
      0x56,  0x2c,   0x2,    0x4c5,  0x4b8,  0x3,    0x2,    0x2,    0x2,
      0x4c5, 0x4c4,  0x3,    0x2,    0x2,    0x2,    0x4c6,  0x3f,   0x3,
      0x2,   0x2,    0x2,    0x4c7,  0x4c9,  0x5,    0x9c,   0x4f,   0x2,
      0x4c8, 0x4ca,  0x5,    0x52,   0x2a,   0x2,    0x4c9,  0x4c8,  0x3,
      0x2,   0x2,    0x2,    0x4c9,  0x4ca,  0x3,    0x2,    0x2,    0x2,
      0x4ca, 0x4cb,  0x3,    0x2,    0x2,    0x2,    0x4cb,  0x4cc,  0x7,
      0x15,  0x2,    0x2,    0x4cc,  0x4cd,  0x7,    0x4,    0x2,    0x2,
      0x4cd, 0x4ce,  0x5,    0xa,    0x6,    0x2,    0x4ce,  0x4cf,  0x7,
      0x5,   0x2,    0x2,    0x4cf,  0x41,   0x3,    0x2,    0x2,    0x2,
      0x4d0, 0x4d1,  0x9,    0xb,    0x2,    0x2,    0x4d1,  0x43,   0x3,
      0x2,   0x2,    0x2,    0x4d2,  0x4d7,  0x5,    0x56,   0x2c,   0x2,
      0x4d3, 0x4d5,  0x7,    0x15,   0x2,    0x2,    0x4d4,  0x4d3,  0x3,
      0x2,   0x2,    0x2,    0x4d4,  0x4d5,  0x3,    0x2,    0x2,    0x2,
      0x4d5, 0x4d6,  0x3,    0x2,    0x2,    0x2,    0x4d6,  0x4d8,  0x5,
      0x9c,  0x4f,   0x2,    0x4d7,  0x4d4,  0x3,    0x2,    0x2,    0x2,
      0x4d7, 0x4d8,  0x3,    0x2,    0x2,    0x2,    0x4d8,  0x4df,  0x3,
      0x2,   0x2,    0x2,    0x4d9,  0x4da,  0x5,    0x90,   0x49,   0x2,
      0x4da, 0x4db,  0x7,    0x3,    0x2,    0x2,    0x4db,  0x4dc,  0x7,
      0xf0,  0x2,    0x2,    0x4dc,  0x4df,  0x3,    0x2,    0x2,    0x2,
      0x4dd, 0x4df,  0x7,    0xf0,   0x2,    0x2,    0x4de,  0x4d2,  0x3,
      0x2,   0x2,    0x2,    0x4de,  0x4d9,  0x3,    0x2,    0x2,    0x2,
      0x4de, 0x4dd,  0x3,    0x2,    0x2,    0x2,    0x4df,  0x45,   0x3,
      0x2,   0x2,    0x2,    0x4e0,  0x4e1,  0x8,    0x24,   0x1,    0x2,
      0x4e1, 0x4e2,  0x5,    0x4c,   0x27,   0x2,    0x4e2,  0x4f5,  0x3,
      0x2,   0x2,    0x2,    0x4e3,  0x4f1,  0xc,    0x4,    0x2,    0x2,
      0x4e4, 0x4e5,  0x7,    0x29,   0x2,    0x2,    0x4e5,  0x4e6,  0x7,
      0x6d,  0x2,    0x2,    0x4e6,  0x4f2,  0x5,    0x4c,   0x27,   0x2,
      0x4e7, 0x4e8,  0x5,    0x48,   0x25,   0x2,    0x4e8,  0x4e9,  0x7,
      0x6d,  0x2,    0x2,    0x4e9,  0x4ea,  0x5,    0x46,   0x24,   0x2,
      0x4ea, 0x4eb,  0x5,    0x4a,   0x26,   0x2,    0x4eb,  0x4f2,  0x3,
      0x2,   0x2,    0x2,    0x4ec,  0x4ed,  0x7,    0x7e,   0x2,    0x2,
      0x4ed, 0x4ee,  0x5,    0x48,   0x25,   0x2,    0x4ee,  0x4ef,  0x7,
      0x6d,  0x2,    0x2,    0x4ef,  0x4f0,  0x5,    0x4c,   0x27,   0x2,
      0x4f0, 0x4f2,  0x3,    0x2,    0x2,    0x2,    0x4f1,  0x4e4,  0x3,
      0x2,   0x2,    0x2,    0x4f1,  0x4e7,  0x3,    0x2,    0x2,    0x2,
      0x4f1, 0x4ec,  0x3,    0x2,    0x2,    0x2,    0x4f2,  0x4f4,  0x3,
      0x2,   0x2,    0x2,    0x4f3,  0x4e3,  0x3,    0x2,    0x2,    0x2,
      0x4f4, 0x4f7,  0x3,    0x2,    0x2,    0x2,    0x4f5,  0x4f3,  0x3,
      0x2,   0x2,    0x2,    0x4f5,  0x4f6,  0x3,    0x2,    0x2,    0x2,
      0x4f6, 0x47,   0x3,    0x2,    0x2,    0x2,    0x4f7,  0x4f5,  0x3,
      0x2,   0x2,    0x2,    0x4f8,  0x4fa,  0x7,    0x62,   0x2,    0x2,
      0x4f9, 0x4f8,  0x3,    0x2,    0x2,    0x2,    0x4f9,  0x4fa,  0x3,
      0x2,   0x2,    0x2,    0x4fa,  0x508,  0x3,    0x2,    0x2,    0x2,
      0x4fb, 0x4fd,  0x7,    0x72,   0x2,    0x2,    0x4fc,  0x4fe,  0x7,
      0x92,  0x2,    0x2,    0x4fd,  0x4fc,  0x3,    0x2,    0x2,    0x2,
      0x4fd, 0x4fe,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x508,  0x3,
      0x2,   0x2,    0x2,    0x4ff,  0x501,  0x7,    0xab,   0x2,    0x2,
      0x500, 0x502,  0x7,    0x92,   0x2,    0x2,    0x501,  0x500,  0x3,
      0x2,   0x2,    0x2,    0x501,  0x502,  0x3,    0x2,    0x2,    0x2,
      0x502, 0x508,  0x3,    0x2,    0x2,    0x2,    0x503,  0x505,  0x7,
      0x52,  0x2,    0x2,    0x504,  0x506,  0x7,    0x92,   0x2,    0x2,
      0x505, 0x504,  0x3,    0x2,    0x2,    0x2,    0x505,  0x506,  0x3,
      0x2,   0x2,    0x2,    0x506,  0x508,  0x3,    0x2,    0x2,    0x2,
      0x507, 0x4f9,  0x3,    0x2,    0x2,    0x2,    0x507,  0x4fb,  0x3,
      0x2,   0x2,    0x2,    0x507,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x507, 0x503,  0x3,    0x2,    0x2,    0x2,    0x508,  0x49,   0x3,
      0x2,   0x2,    0x2,    0x509,  0x50a,  0x7,    0x8c,   0x2,    0x2,
      0x50a, 0x518,  0x5,    0x58,   0x2d,   0x2,    0x50b,  0x50c,  0x7,
      0xdb,  0x2,    0x2,    0x50c,  0x50d,  0x7,    0x4,    0x2,    0x2,
      0x50d, 0x512,  0x5,    0x9c,   0x4f,   0x2,    0x50e,  0x50f,  0x7,
      0x6,   0x2,    0x2,    0x50f,  0x511,  0x5,    0x9c,   0x4f,   0x2,
      0x510, 0x50e,  0x3,    0x2,    0x2,    0x2,    0x511,  0x514,  0x3,
      0x2,   0x2,    0x2,    0x512,  0x510,  0x3,    0x2,    0x2,    0x2,
      0x512, 0x513,  0x3,    0x2,    0x2,    0x2,    0x513,  0x515,  0x3,
      0x2,   0x2,    0x2,    0x514,  0x512,  0x3,    0x2,    0x2,    0x2,
      0x515, 0x516,  0x7,    0x5,    0x2,    0x2,    0x516,  0x518,  0x3,
      0x2,   0x2,    0x2,    0x517,  0x509,  0x3,    0x2,    0x2,    0x2,
      0x517, 0x50b,  0x3,    0x2,    0x2,    0x2,    0x518,  0x4b,   0x3,
      0x2,   0x2,    0x2,    0x519,  0x520,  0x5,    0x50,   0x29,   0x2,
      0x51a, 0x51b,  0x7,    0xc6,   0x2,    0x2,    0x51b,  0x51c,  0x5,
      0x4e,  0x28,   0x2,    0x51c,  0x51d,  0x7,    0x4,    0x2,    0x2,
      0x51d, 0x51e,  0x5,    0x56,   0x2c,   0x2,    0x51e,  0x51f,  0x7,
      0x5,   0x2,    0x2,    0x51f,  0x521,  0x3,    0x2,    0x2,    0x2,
      0x520, 0x51a,  0x3,    0x2,    0x2,    0x2,    0x520,  0x521,  0x3,
      0x2,   0x2,    0x2,    0x521,  0x4d,   0x3,    0x2,    0x2,    0x2,
      0x522, 0x523,  0x9,    0xc,    0x2,    0x2,    0x523,  0x4f,   0x3,
      0x2,   0x2,    0x2,    0x524,  0x52c,  0x5,    0x54,   0x2b,   0x2,
      0x525, 0x527,  0x7,    0x15,   0x2,    0x2,    0x526,  0x525,  0x3,
      0x2,   0x2,    0x2,    0x526,  0x527,  0x3,    0x2,    0x2,    0x2,
      0x527, 0x528,  0x3,    0x2,    0x2,    0x2,    0x528,  0x52a,  0x5,
      0x9c,  0x4f,   0x2,    0x529,  0x52b,  0x5,    0x52,   0x2a,   0x2,
      0x52a, 0x529,  0x3,    0x2,    0x2,    0x2,    0x52a,  0x52b,  0x3,
      0x2,   0x2,    0x2,    0x52b,  0x52d,  0x3,    0x2,    0x2,    0x2,
      0x52c, 0x526,  0x3,    0x2,    0x2,    0x2,    0x52c,  0x52d,  0x3,
      0x2,   0x2,    0x2,    0x52d,  0x51,   0x3,    0x2,    0x2,    0x2,
      0x52e, 0x52f,  0x7,    0x4,    0x2,    0x2,    0x52f,  0x534,  0x5,
      0x9c,  0x4f,   0x2,    0x530,  0x531,  0x7,    0x6,    0x2,    0x2,
      0x531, 0x533,  0x5,    0x9c,   0x4f,   0x2,    0x532,  0x530,  0x3,
      0x2,   0x2,    0x2,    0x533,  0x536,  0x3,    0x2,    0x2,    0x2,
      0x534, 0x532,  0x3,    0x2,    0x2,    0x2,    0x534,  0x535,  0x3,
      0x2,   0x2,    0x2,    0x535,  0x537,  0x3,    0x2,    0x2,    0x2,
      0x536, 0x534,  0x3,    0x2,    0x2,    0x2,    0x537,  0x538,  0x7,
      0x5,   0x2,    0x2,    0x538,  0x53,   0x3,    0x2,    0x2,    0x2,
      0x539, 0x53b,  0x5,    0x90,   0x49,   0x2,    0x53a,  0x53c,  0x5,
      0x92,  0x4a,   0x2,    0x53b,  0x53a,  0x3,    0x2,    0x2,    0x2,
      0x53b, 0x53c,  0x3,    0x2,    0x2,    0x2,    0x53c,  0x55a,  0x3,
      0x2,   0x2,    0x2,    0x53d,  0x53e,  0x7,    0x4,    0x2,    0x2,
      0x53e, 0x53f,  0x5,    0xa,    0x6,    0x2,    0x53f,  0x540,  0x7,
      0x5,   0x2,    0x2,    0x540,  0x55a,  0x3,    0x2,    0x2,    0x2,
      0x541, 0x542,  0x7,    0xd7,   0x2,    0x2,    0x542,  0x543,  0x7,
      0x4,   0x2,    0x2,    0x543,  0x548,  0x5,    0x56,   0x2c,   0x2,
      0x544, 0x545,  0x7,    0x6,    0x2,    0x2,    0x545,  0x547,  0x5,
      0x56,  0x2c,   0x2,    0x546,  0x544,  0x3,    0x2,    0x2,    0x2,
      0x547, 0x54a,  0x3,    0x2,    0x2,    0x2,    0x548,  0x546,  0x3,
      0x2,   0x2,    0x2,    0x548,  0x549,  0x3,    0x2,    0x2,    0x2,
      0x549, 0x54b,  0x3,    0x2,    0x2,    0x2,    0x54a,  0x548,  0x3,
      0x2,   0x2,    0x2,    0x54b,  0x54e,  0x7,    0x5,    0x2,    0x2,
      0x54c, 0x54d,  0x7,    0xe3,   0x2,    0x2,    0x54d,  0x54f,  0x7,
      0x91,  0x2,    0x2,    0x54e,  0x54c,  0x3,    0x2,    0x2,    0x2,
      0x54e, 0x54f,  0x3,    0x2,    0x2,    0x2,    0x54f,  0x55a,  0x3,
      0x2,   0x2,    0x2,    0x550,  0x551,  0x7,    0x71,   0x2,    0x2,
      0x551, 0x552,  0x7,    0x4,    0x2,    0x2,    0x552,  0x553,  0x5,
      0xa,   0x6,    0x2,    0x553,  0x554,  0x7,    0x5,    0x2,    0x2,
      0x554, 0x55a,  0x3,    0x2,    0x2,    0x2,    0x555,  0x556,  0x7,
      0x4,   0x2,    0x2,    0x556,  0x557,  0x5,    0x46,   0x24,   0x2,
      0x557, 0x558,  0x7,    0x5,    0x2,    0x2,    0x558,  0x55a,  0x3,
      0x2,   0x2,    0x2,    0x559,  0x539,  0x3,    0x2,    0x2,    0x2,
      0x559, 0x53d,  0x3,    0x2,    0x2,    0x2,    0x559,  0x541,  0x3,
      0x2,   0x2,    0x2,    0x559,  0x550,  0x3,    0x2,    0x2,    0x2,
      0x559, 0x555,  0x3,    0x2,    0x2,    0x2,    0x55a,  0x55,   0x3,
      0x2,   0x2,    0x2,    0x55b,  0x55c,  0x5,    0x58,   0x2d,   0x2,
      0x55c, 0x57,   0x3,    0x2,    0x2,    0x2,    0x55d,  0x55e,  0x8,
      0x2d,  0x1,    0x2,    0x55e,  0x560,  0x5,    0x5c,   0x2f,   0x2,
      0x55f, 0x561,  0x5,    0x5a,   0x2e,   0x2,    0x560,  0x55f,  0x3,
      0x2,   0x2,    0x2,    0x560,  0x561,  0x3,    0x2,    0x2,    0x2,
      0x561, 0x565,  0x3,    0x2,    0x2,    0x2,    0x562,  0x563,  0x7,
      0x86,  0x2,    0x2,    0x563,  0x565,  0x5,    0x58,   0x2d,   0x5,
      0x564, 0x55d,  0x3,    0x2,    0x2,    0x2,    0x564,  0x562,  0x3,
      0x2,   0x2,    0x2,    0x565,  0x56e,  0x3,    0x2,    0x2,    0x2,
      0x566, 0x567,  0xc,    0x4,    0x2,    0x2,    0x567,  0x568,  0x7,
      0x12,  0x2,    0x2,    0x568,  0x56d,  0x5,    0x58,   0x2d,   0x5,
      0x569, 0x56a,  0xc,    0x3,    0x2,    0x2,    0x56a,  0x56b,  0x7,
      0x8f,  0x2,    0x2,    0x56b,  0x56d,  0x5,    0x58,   0x2d,   0x4,
      0x56c, 0x566,  0x3,    0x2,    0x2,    0x2,    0x56c,  0x569,  0x3,
      0x2,   0x2,    0x2,    0x56d,  0x570,  0x3,    0x2,    0x2,    0x2,
      0x56e, 0x56c,  0x3,    0x2,    0x2,    0x2,    0x56e,  0x56f,  0x3,
      0x2,   0x2,    0x2,    0x56f,  0x59,   0x3,    0x2,    0x2,    0x2,
      0x570, 0x56e,  0x3,    0x2,    0x2,    0x2,    0x571,  0x572,  0x5,
      0x66,  0x34,   0x2,    0x572,  0x573,  0x5,    0x5c,   0x2f,   0x2,
      0x573, 0x5af,  0x3,    0x2,    0x2,    0x2,    0x574,  0x575,  0x5,
      0x66,  0x34,   0x2,    0x575,  0x576,  0x5,    0x68,   0x35,   0x2,
      0x576, 0x577,  0x7,    0x4,    0x2,    0x2,    0x577,  0x578,  0x5,
      0xa,   0x6,    0x2,    0x578,  0x579,  0x7,    0x5,    0x2,    0x2,
      0x579, 0x5af,  0x3,    0x2,    0x2,    0x2,    0x57a,  0x57c,  0x7,
      0x86,  0x2,    0x2,    0x57b,  0x57a,  0x3,    0x2,    0x2,    0x2,
      0x57b, 0x57c,  0x3,    0x2,    0x2,    0x2,    0x57c,  0x57d,  0x3,
      0x2,   0x2,    0x2,    0x57d,  0x57e,  0x7,    0x1a,   0x2,    0x2,
      0x57e, 0x57f,  0x5,    0x5c,   0x2f,   0x2,    0x57f,  0x580,  0x7,
      0x12,  0x2,    0x2,    0x580,  0x581,  0x5,    0x5c,   0x2f,   0x2,
      0x581, 0x5af,  0x3,    0x2,    0x2,    0x2,    0x582,  0x584,  0x7,
      0x86,  0x2,    0x2,    0x583,  0x582,  0x3,    0x2,    0x2,    0x2,
      0x583, 0x584,  0x3,    0x2,    0x2,    0x2,    0x584,  0x585,  0x3,
      0x2,   0x2,    0x2,    0x585,  0x586,  0x7,    0x60,   0x2,    0x2,
      0x586, 0x587,  0x7,    0x4,    0x2,    0x2,    0x587,  0x58c,  0x5,
      0x56,  0x2c,   0x2,    0x588,  0x589,  0x7,    0x6,    0x2,    0x2,
      0x589, 0x58b,  0x5,    0x56,   0x2c,   0x2,    0x58a,  0x588,  0x3,
      0x2,   0x2,    0x2,    0x58b,  0x58e,  0x3,    0x2,    0x2,    0x2,
      0x58c, 0x58a,  0x3,    0x2,    0x2,    0x2,    0x58c,  0x58d,  0x3,
      0x2,   0x2,    0x2,    0x58d,  0x58f,  0x3,    0x2,    0x2,    0x2,
      0x58e, 0x58c,  0x3,    0x2,    0x2,    0x2,    0x58f,  0x590,  0x7,
      0x5,   0x2,    0x2,    0x590,  0x5af,  0x3,    0x2,    0x2,    0x2,
      0x591, 0x593,  0x7,    0x86,   0x2,    0x2,    0x592,  0x591,  0x3,
      0x2,   0x2,    0x2,    0x592,  0x593,  0x3,    0x2,    0x2,    0x2,
      0x593, 0x594,  0x3,    0x2,    0x2,    0x2,    0x594,  0x595,  0x7,
      0x60,  0x2,    0x2,    0x595,  0x596,  0x7,    0x4,    0x2,    0x2,
      0x596, 0x597,  0x5,    0xa,    0x6,    0x2,    0x597,  0x598,  0x7,
      0x5,   0x2,    0x2,    0x598,  0x5af,  0x3,    0x2,    0x2,    0x2,
      0x599, 0x59b,  0x7,    0x86,   0x2,    0x2,    0x59a,  0x599,  0x3,
      0x2,   0x2,    0x2,    0x59a,  0x59b,  0x3,    0x2,    0x2,    0x2,
      0x59b, 0x59c,  0x3,    0x2,    0x2,    0x2,    0x59c,  0x59d,  0x7,
      0x74,  0x2,    0x2,    0x59d,  0x5a0,  0x5,    0x5c,   0x2f,   0x2,
      0x59e, 0x59f,  0x7,    0x42,   0x2,    0x2,    0x59f,  0x5a1,  0x5,
      0x5c,  0x2f,   0x2,    0x5a0,  0x59e,  0x3,    0x2,    0x2,    0x2,
      0x5a0, 0x5a1,  0x3,    0x2,    0x2,    0x2,    0x5a1,  0x5af,  0x3,
      0x2,   0x2,    0x2,    0x5a2,  0x5a4,  0x7,    0x6a,   0x2,    0x2,
      0x5a3, 0x5a5,  0x7,    0x86,   0x2,    0x2,    0x5a4,  0x5a3,  0x3,
      0x2,   0x2,    0x2,    0x5a4,  0x5a5,  0x3,    0x2,    0x2,    0x2,
      0x5a5, 0x5a6,  0x3,    0x2,    0x2,    0x2,    0x5a6,  0x5af,  0x7,
      0x87,  0x2,    0x2,    0x5a7,  0x5a9,  0x7,    0x6a,   0x2,    0x2,
      0x5a8, 0x5aa,  0x7,    0x86,   0x2,    0x2,    0x5a9,  0x5a8,  0x3,
      0x2,   0x2,    0x2,    0x5a9,  0x5aa,  0x3,    0x2,    0x2,    0x2,
      0x5aa, 0x5ab,  0x3,    0x2,    0x2,    0x2,    0x5ab,  0x5ac,  0x7,
      0x3b,  0x2,    0x2,    0x5ac,  0x5ad,  0x7,    0x51,   0x2,    0x2,
      0x5ad, 0x5af,  0x5,    0x5c,   0x2f,   0x2,    0x5ae,  0x571,  0x3,
      0x2,   0x2,    0x2,    0x5ae,  0x574,  0x3,    0x2,    0x2,    0x2,
      0x5ae, 0x57b,  0x3,    0x2,    0x2,    0x2,    0x5ae,  0x583,  0x3,
      0x2,   0x2,    0x2,    0x5ae,  0x592,  0x3,    0x2,    0x2,    0x2,
      0x5ae, 0x59a,  0x3,    0x2,    0x2,    0x2,    0x5ae,  0x5a2,  0x3,
      0x2,   0x2,    0x2,    0x5ae,  0x5a7,  0x3,    0x2,    0x2,    0x2,
      0x5af, 0x5b,   0x3,    0x2,    0x2,    0x2,    0x5b0,  0x5b1,  0x8,
      0x2f,  0x1,    0x2,    0x5b1,  0x5b5,  0x5,    0x5e,   0x30,   0x2,
      0x5b2, 0x5b3,  0x9,    0xd,    0x2,    0x2,    0x5b3,  0x5b5,  0x5,
      0x5c,  0x2f,   0x6,    0x5b4,  0x5b0,  0x3,    0x2,    0x2,    0x2,
      0x5b4, 0x5b2,  0x3,    0x2,    0x2,    0x2,    0x5b5,  0x5c4,  0x3,
      0x2,   0x2,    0x2,    0x5b6,  0x5b7,  0xc,    0x5,    0x2,    0x2,
      0x5b7, 0x5b8,  0x9,    0xe,    0x2,    0x2,    0x5b8,  0x5c3,  0x5,
      0x5c,  0x2f,   0x6,    0x5b9,  0x5ba,  0xc,    0x4,    0x2,    0x2,
      0x5ba, 0x5bb,  0x9,    0xd,    0x2,    0x2,    0x5bb,  0x5c3,  0x5,
      0x5c,  0x2f,   0x5,    0x5bc,  0x5bd,  0xc,    0x3,    0x2,    0x2,
      0x5bd, 0x5be,  0x7,    0xf3,   0x2,    0x2,    0x5be,  0x5c3,  0x5,
      0x5c,  0x2f,   0x4,    0x5bf,  0x5c0,  0xc,    0x7,    0x2,    0x2,
      0x5c0, 0x5c1,  0x7,    0x17,   0x2,    0x2,    0x5c1,  0x5c3,  0x5,
      0x64,  0x33,   0x2,    0x5c2,  0x5b6,  0x3,    0x2,    0x2,    0x2,
      0x5c2, 0x5b9,  0x3,    0x2,    0x2,    0x2,    0x5c2,  0x5bc,  0x3,
      0x2,   0x2,    0x2,    0x5c2,  0x5bf,  0x3,    0x2,    0x2,    0x2,
      0x5c3, 0x5c6,  0x3,    0x2,    0x2,    0x2,    0x5c4,  0x5c2,  0x3,
      0x2,   0x2,    0x2,    0x5c4,  0x5c5,  0x3,    0x2,    0x2,    0x2,
      0x5c5, 0x5d,   0x3,    0x2,    0x2,    0x2,    0x5c6,  0x5c4,  0x3,
      0x2,   0x2,    0x2,    0x5c7,  0x5c8,  0x8,    0x30,   0x1,    0x2,
      0x5c8, 0x6b7,  0x7,    0x87,   0x2,    0x2,    0x5c9,  0x6b7,  0x5,
      0x6c,  0x37,   0x2,    0x5ca,  0x5cb,  0x5,    0x9c,   0x4f,   0x2,
      0x5cb, 0x5cc,  0x5,    0x60,   0x31,   0x2,    0x5cc,  0x6b7,  0x3,
      0x2,   0x2,    0x2,    0x5cd,  0x5ce,  0x7,    0x100,  0x2,    0x2,
      0x5ce, 0x6b7,  0x5,    0x60,   0x31,   0x2,    0x5cf,  0x6b7,  0x5,
      0x9e,  0x50,   0x2,    0x5d0,  0x6b7,  0x5,    0x6a,   0x36,   0x2,
      0x5d1, 0x6b7,  0x5,    0x60,   0x31,   0x2,    0x5d2,  0x6b7,  0x7,
      0xf6,  0x2,    0x2,    0x5d3,  0x6b7,  0x7,    0x7,    0x2,    0x2,
      0x5d4, 0x5d5,  0x7,    0x97,   0x2,    0x2,    0x5d5,  0x5d6,  0x7,
      0x4,   0x2,    0x2,    0x5d6,  0x5d7,  0x5,    0x5c,   0x2f,   0x2,
      0x5d7, 0x5d8,  0x7,    0x60,   0x2,    0x2,    0x5d8,  0x5d9,  0x5,
      0x5c,  0x2f,   0x2,    0x5d9,  0x5da,  0x7,    0x5,    0x2,    0x2,
      0x5da, 0x6b7,  0x3,    0x2,    0x2,    0x2,    0x5db,  0x5dc,  0x7,
      0x4,   0x2,    0x2,    0x5dc,  0x5df,  0x5,    0x56,   0x2c,   0x2,
      0x5dd, 0x5de,  0x7,    0x6,    0x2,    0x2,    0x5de,  0x5e0,  0x5,
      0x56,  0x2c,   0x2,    0x5df,  0x5dd,  0x3,    0x2,    0x2,    0x2,
      0x5e0, 0x5e1,  0x3,    0x2,    0x2,    0x2,    0x5e1,  0x5df,  0x3,
      0x2,   0x2,    0x2,    0x5e1,  0x5e2,  0x3,    0x2,    0x2,    0x2,
      0x5e2, 0x5e3,  0x3,    0x2,    0x2,    0x2,    0x5e3,  0x5e4,  0x7,
      0x5,   0x2,    0x2,    0x5e4,  0x6b7,  0x3,    0x2,    0x2,    0x2,
      0x5e5, 0x5e6,  0x7,    0xb0,   0x2,    0x2,    0x5e6,  0x5e7,  0x7,
      0x4,   0x2,    0x2,    0x5e7,  0x5ec,  0x5,    0x56,   0x2c,   0x2,
      0x5e8, 0x5e9,  0x7,    0x6,    0x2,    0x2,    0x5e9,  0x5eb,  0x5,
      0x56,  0x2c,   0x2,    0x5ea,  0x5e8,  0x3,    0x2,    0x2,    0x2,
      0x5eb, 0x5ee,  0x3,    0x2,    0x2,    0x2,    0x5ec,  0x5ea,  0x3,
      0x2,   0x2,    0x2,    0x5ec,  0x5ed,  0x3,    0x2,    0x2,    0x2,
      0x5ed, 0x5ef,  0x3,    0x2,    0x2,    0x2,    0x5ee,  0x5ec,  0x3,
      0x2,   0x2,    0x2,    0x5ef,  0x5f0,  0x7,    0x5,    0x2,    0x2,
      0x5f0, 0x6b7,  0x3,    0x2,    0x2,    0x2,    0x5f1,  0x5f2,  0x5,
      0x90,  0x49,   0x2,    0x5f2,  0x5f3,  0x7,    0x4,    0x2,    0x2,
      0x5f3, 0x5f4,  0x7,    0xf0,   0x2,    0x2,    0x5f4,  0x5f6,  0x7,
      0x5,   0x2,    0x2,    0x5f5,  0x5f7,  0x5,    0x7c,   0x3f,   0x2,
      0x5f6, 0x5f5,  0x3,    0x2,    0x2,    0x2,    0x5f6,  0x5f7,  0x3,
      0x2,   0x2,    0x2,    0x5f7,  0x5f9,  0x3,    0x2,    0x2,    0x2,
      0x5f8, 0x5fa,  0x5,    0x7e,   0x40,   0x2,    0x5f9,  0x5f8,  0x3,
      0x2,   0x2,    0x2,    0x5f9,  0x5fa,  0x3,    0x2,    0x2,    0x2,
      0x5fa, 0x6b7,  0x3,    0x2,    0x2,    0x2,    0x5fb,  0x5fc,  0x5,
      0x90,  0x49,   0x2,    0x5fc,  0x608,  0x7,    0x4,    0x2,    0x2,
      0x5fd, 0x5ff,  0x5,    0x42,   0x22,   0x2,    0x5fe,  0x5fd,  0x3,
      0x2,   0x2,    0x2,    0x5fe,  0x5ff,  0x3,    0x2,    0x2,    0x2,
      0x5ff, 0x600,  0x3,    0x2,    0x2,    0x2,    0x600,  0x605,  0x5,
      0x56,  0x2c,   0x2,    0x601,  0x602,  0x7,    0x6,    0x2,    0x2,
      0x602, 0x604,  0x5,    0x56,   0x2c,   0x2,    0x603,  0x601,  0x3,
      0x2,   0x2,    0x2,    0x604,  0x607,  0x3,    0x2,    0x2,    0x2,
      0x605, 0x603,  0x3,    0x2,    0x2,    0x2,    0x605,  0x606,  0x3,
      0x2,   0x2,    0x2,    0x606,  0x609,  0x3,    0x2,    0x2,    0x2,
      0x607, 0x605,  0x3,    0x2,    0x2,    0x2,    0x608,  0x5fe,  0x3,
      0x2,   0x2,    0x2,    0x608,  0x609,  0x3,    0x2,    0x2,    0x2,
      0x609, 0x614,  0x3,    0x2,    0x2,    0x2,    0x60a,  0x60b,  0x7,
      0x90,  0x2,    0x2,    0x60b,  0x60c,  0x7,    0x1b,   0x2,    0x2,
      0x60c, 0x611,  0x5,    0x36,   0x1c,   0x2,    0x60d,  0x60e,  0x7,
      0x6,   0x2,    0x2,    0x60e,  0x610,  0x5,    0x36,   0x1c,   0x2,
      0x60f, 0x60d,  0x3,    0x2,    0x2,    0x2,    0x610,  0x613,  0x3,
      0x2,   0x2,    0x2,    0x611,  0x60f,  0x3,    0x2,    0x2,    0x2,
      0x611, 0x612,  0x3,    0x2,    0x2,    0x2,    0x612,  0x615,  0x3,
      0x2,   0x2,    0x2,    0x613,  0x611,  0x3,    0x2,    0x2,    0x2,
      0x614, 0x60a,  0x3,    0x2,    0x2,    0x2,    0x614,  0x615,  0x3,
      0x2,   0x2,    0x2,    0x615,  0x616,  0x3,    0x2,    0x2,    0x2,
      0x616, 0x618,  0x7,    0x5,    0x2,    0x2,    0x617,  0x619,  0x5,
      0x7c,  0x3f,   0x2,    0x618,  0x617,  0x3,    0x2,    0x2,    0x2,
      0x618, 0x619,  0x3,    0x2,    0x2,    0x2,    0x619,  0x61e,  0x3,
      0x2,   0x2,    0x2,    0x61a,  0x61c,  0x5,    0x62,   0x32,   0x2,
      0x61b, 0x61a,  0x3,    0x2,    0x2,    0x2,    0x61b,  0x61c,  0x3,
      0x2,   0x2,    0x2,    0x61c,  0x61d,  0x3,    0x2,    0x2,    0x2,
      0x61d, 0x61f,  0x5,    0x7e,   0x40,   0x2,    0x61e,  0x61b,  0x3,
      0x2,   0x2,    0x2,    0x61e,  0x61f,  0x3,    0x2,    0x2,    0x2,
      0x61f, 0x6b7,  0x3,    0x2,    0x2,    0x2,    0x620,  0x621,  0x5,
      0x9c,  0x4f,   0x2,    0x621,  0x622,  0x7,    0x8,    0x2,    0x2,
      0x622, 0x623,  0x5,    0x56,   0x2c,   0x2,    0x623,  0x6b7,  0x3,
      0x2,   0x2,    0x2,    0x624,  0x62d,  0x7,    0x4,    0x2,    0x2,
      0x625, 0x62a,  0x5,    0x9c,   0x4f,   0x2,    0x626,  0x627,  0x7,
      0x6,   0x2,    0x2,    0x627,  0x629,  0x5,    0x9c,   0x4f,   0x2,
      0x628, 0x626,  0x3,    0x2,    0x2,    0x2,    0x629,  0x62c,  0x3,
      0x2,   0x2,    0x2,    0x62a,  0x628,  0x3,    0x2,    0x2,    0x2,
      0x62a, 0x62b,  0x3,    0x2,    0x2,    0x2,    0x62b,  0x62e,  0x3,
      0x2,   0x2,    0x2,    0x62c,  0x62a,  0x3,    0x2,    0x2,    0x2,
      0x62d, 0x625,  0x3,    0x2,    0x2,    0x2,    0x62d,  0x62e,  0x3,
      0x2,   0x2,    0x2,    0x62e,  0x62f,  0x3,    0x2,    0x2,    0x2,
      0x62f, 0x630,  0x7,    0x5,    0x2,    0x2,    0x630,  0x631,  0x7,
      0x8,   0x2,    0x2,    0x631,  0x6b7,  0x5,    0x56,   0x2c,   0x2,
      0x632, 0x633,  0x7,    0x4,    0x2,    0x2,    0x633,  0x634,  0x5,
      0xa,   0x6,    0x2,    0x634,  0x635,  0x7,    0x5,    0x2,    0x2,
      0x635, 0x6b7,  0x3,    0x2,    0x2,    0x2,    0x636,  0x637,  0x7,
      0x46,  0x2,    0x2,    0x637,  0x638,  0x7,    0x4,    0x2,    0x2,
      0x638, 0x639,  0x5,    0xa,    0x6,    0x2,    0x639,  0x63a,  0x7,
      0x5,   0x2,    0x2,    0x63a,  0x6b7,  0x3,    0x2,    0x2,    0x2,
      0x63b, 0x63c,  0x7,    0x1f,   0x2,    0x2,    0x63c,  0x63e,  0x5,
      0x5c,  0x2f,   0x2,    0x63d,  0x63f,  0x5,    0x7a,   0x3e,   0x2,
      0x63e, 0x63d,  0x3,    0x2,    0x2,    0x2,    0x63f,  0x640,  0x3,
      0x2,   0x2,    0x2,    0x640,  0x63e,  0x3,    0x2,    0x2,    0x2,
      0x640, 0x641,  0x3,    0x2,    0x2,    0x2,    0x641,  0x644,  0x3,
      0x2,   0x2,    0x2,    0x642,  0x643,  0x7,    0x3e,   0x2,    0x2,
      0x643, 0x645,  0x5,    0x56,   0x2c,   0x2,    0x644,  0x642,  0x3,
      0x2,   0x2,    0x2,    0x644,  0x645,  0x3,    0x2,    0x2,    0x2,
      0x645, 0x646,  0x3,    0x2,    0x2,    0x2,    0x646,  0x647,  0x7,
      0x40,  0x2,    0x2,    0x647,  0x6b7,  0x3,    0x2,    0x2,    0x2,
      0x648, 0x64a,  0x7,    0x1f,   0x2,    0x2,    0x649,  0x64b,  0x5,
      0x7a,  0x3e,   0x2,    0x64a,  0x649,  0x3,    0x2,    0x2,    0x2,
      0x64b, 0x64c,  0x3,    0x2,    0x2,    0x2,    0x64c,  0x64a,  0x3,
      0x2,   0x2,    0x2,    0x64c,  0x64d,  0x3,    0x2,    0x2,    0x2,
      0x64d, 0x650,  0x3,    0x2,    0x2,    0x2,    0x64e,  0x64f,  0x7,
      0x3e,  0x2,    0x2,    0x64f,  0x651,  0x5,    0x56,   0x2c,   0x2,
      0x650, 0x64e,  0x3,    0x2,    0x2,    0x2,    0x650,  0x651,  0x3,
      0x2,   0x2,    0x2,    0x651,  0x652,  0x3,    0x2,    0x2,    0x2,
      0x652, 0x653,  0x7,    0x40,   0x2,    0x2,    0x653,  0x6b7,  0x3,
      0x2,   0x2,    0x2,    0x654,  0x655,  0x7,    0x20,   0x2,    0x2,
      0x655, 0x656,  0x7,    0x4,    0x2,    0x2,    0x656,  0x657,  0x5,
      0x56,  0x2c,   0x2,    0x657,  0x658,  0x7,    0x15,   0x2,    0x2,
      0x658, 0x659,  0x5,    0x74,   0x3b,   0x2,    0x659,  0x65a,  0x7,
      0x5,   0x2,    0x2,    0x65a,  0x6b7,  0x3,    0x2,    0x2,    0x2,
      0x65b, 0x65c,  0x7,    0xd0,   0x2,    0x2,    0x65c,  0x65d,  0x7,
      0x4,   0x2,    0x2,    0x65d,  0x65e,  0x5,    0x56,   0x2c,   0x2,
      0x65e, 0x65f,  0x7,    0x15,   0x2,    0x2,    0x65f,  0x660,  0x5,
      0x74,  0x3b,   0x2,    0x660,  0x661,  0x7,    0x5,    0x2,    0x2,
      0x661, 0x6b7,  0x3,    0x2,    0x2,    0x2,    0x662,  0x663,  0x7,
      0x14,  0x2,    0x2,    0x663,  0x66c,  0x7,    0x9,    0x2,    0x2,
      0x664, 0x669,  0x5,    0x56,   0x2c,   0x2,    0x665,  0x666,  0x7,
      0x6,   0x2,    0x2,    0x666,  0x668,  0x5,    0x56,   0x2c,   0x2,
      0x667, 0x665,  0x3,    0x2,    0x2,    0x2,    0x668,  0x66b,  0x3,
      0x2,   0x2,    0x2,    0x669,  0x667,  0x3,    0x2,    0x2,    0x2,
      0x669, 0x66a,  0x3,    0x2,    0x2,    0x2,    0x66a,  0x66d,  0x3,
      0x2,   0x2,    0x2,    0x66b,  0x669,  0x3,    0x2,    0x2,    0x2,
      0x66c, 0x664,  0x3,    0x2,    0x2,    0x2,    0x66c,  0x66d,  0x3,
      0x2,   0x2,    0x2,    0x66d,  0x66e,  0x3,    0x2,    0x2,    0x2,
      0x66e, 0x6b7,  0x7,    0xa,    0x2,    0x2,    0x66f,  0x6b7,  0x5,
      0x9c,  0x4f,   0x2,    0x670,  0x6b7,  0x7,    0x2c,   0x2,    0x2,
      0x671, 0x675,  0x7,    0x2e,   0x2,    0x2,    0x672,  0x673,  0x7,
      0x4,   0x2,    0x2,    0x673,  0x674,  0x7,    0xf7,   0x2,    0x2,
      0x674, 0x676,  0x7,    0x5,    0x2,    0x2,    0x675,  0x672,  0x3,
      0x2,   0x2,    0x2,    0x675,  0x676,  0x3,    0x2,    0x2,    0x2,
      0x676, 0x6b7,  0x3,    0x2,    0x2,    0x2,    0x677,  0x67b,  0x7,
      0x2f,  0x2,    0x2,    0x678,  0x679,  0x7,    0x4,    0x2,    0x2,
      0x679, 0x67a,  0x7,    0xf7,   0x2,    0x2,    0x67a,  0x67c,  0x7,
      0x5,   0x2,    0x2,    0x67b,  0x678,  0x3,    0x2,    0x2,    0x2,
      0x67b, 0x67c,  0x3,    0x2,    0x2,    0x2,    0x67c,  0x6b7,  0x3,
      0x2,   0x2,    0x2,    0x67d,  0x681,  0x7,    0x76,   0x2,    0x2,
      0x67e, 0x67f,  0x7,    0x4,    0x2,    0x2,    0x67f,  0x680,  0x7,
      0xf7,  0x2,    0x2,    0x680,  0x682,  0x7,    0x5,    0x2,    0x2,
      0x681, 0x67e,  0x3,    0x2,    0x2,    0x2,    0x681,  0x682,  0x3,
      0x2,   0x2,    0x2,    0x682,  0x6b7,  0x3,    0x2,    0x2,    0x2,
      0x683, 0x687,  0x7,    0x77,   0x2,    0x2,    0x684,  0x685,  0x7,
      0x4,   0x2,    0x2,    0x685,  0x686,  0x7,    0xf7,   0x2,    0x2,
      0x686, 0x688,  0x7,    0x5,    0x2,    0x2,    0x687,  0x684,  0x3,
      0x2,   0x2,    0x2,    0x687,  0x688,  0x3,    0x2,    0x2,    0x2,
      0x688, 0x6b7,  0x3,    0x2,    0x2,    0x2,    0x689,  0x6b7,  0x7,
      0x30,  0x2,    0x2,    0x68a,  0x68b,  0x7,    0xc0,   0x2,    0x2,
      0x68b, 0x68c,  0x7,    0x4,    0x2,    0x2,    0x68c,  0x68d,  0x5,
      0x5c,  0x2f,   0x2,    0x68d,  0x68e,  0x7,    0x51,   0x2,    0x2,
      0x68e, 0x691,  0x5,    0x5c,   0x2f,   0x2,    0x68f,  0x690,  0x7,
      0x4f,  0x2,    0x2,    0x690,  0x692,  0x5,    0x5c,   0x2f,   0x2,
      0x691, 0x68f,  0x3,    0x2,    0x2,    0x2,    0x691,  0x692,  0x3,
      0x2,   0x2,    0x2,    0x692,  0x693,  0x3,    0x2,    0x2,    0x2,
      0x693, 0x694,  0x7,    0x5,    0x2,    0x2,    0x694,  0x6b7,  0x3,
      0x2,   0x2,    0x2,    0x695,  0x696,  0x7,    0x85,   0x2,    0x2,
      0x696, 0x697,  0x7,    0x4,    0x2,    0x2,    0x697,  0x69a,  0x5,
      0x5c,  0x2f,   0x2,    0x698,  0x699,  0x7,    0x6,    0x2,    0x2,
      0x699, 0x69b,  0x5,    0x70,   0x39,   0x2,    0x69a,  0x698,  0x3,
      0x2,   0x2,    0x2,    0x69a,  0x69b,  0x3,    0x2,    0x2,    0x2,
      0x69b, 0x69c,  0x3,    0x2,    0x2,    0x2,    0x69c,  0x69d,  0x7,
      0x5,   0x2,    0x2,    0x69d,  0x6b7,  0x3,    0x2,    0x2,    0x2,
      0x69e, 0x69f,  0x7,    0x48,   0x2,    0x2,    0x69f,  0x6a0,  0x7,
      0x4,   0x2,    0x2,    0x6a0,  0x6a1,  0x5,    0x9c,   0x4f,   0x2,
      0x6a1, 0x6a2,  0x7,    0x51,   0x2,    0x2,    0x6a2,  0x6a3,  0x5,
      0x5c,  0x2f,   0x2,    0x6a3,  0x6a4,  0x7,    0x5,    0x2,    0x2,
      0x6a4, 0x6b7,  0x3,    0x2,    0x2,    0x2,    0x6a5,  0x6a6,  0x7,
      0x4,   0x2,    0x2,    0x6a6,  0x6a7,  0x5,    0x56,   0x2c,   0x2,
      0x6a7, 0x6a8,  0x7,    0x5,    0x2,    0x2,    0x6a8,  0x6b7,  0x3,
      0x2,   0x2,    0x2,    0x6a9,  0x6aa,  0x7,    0x5a,   0x2,    0x2,
      0x6aa, 0x6b3,  0x7,    0x4,    0x2,    0x2,    0x6ab,  0x6b0,  0x5,
      0x90,  0x49,   0x2,    0x6ac,  0x6ad,  0x7,    0x6,    0x2,    0x2,
      0x6ad, 0x6af,  0x5,    0x90,   0x49,   0x2,    0x6ae,  0x6ac,  0x3,
      0x2,   0x2,    0x2,    0x6af,  0x6b2,  0x3,    0x2,    0x2,    0x2,
      0x6b0, 0x6ae,  0x3,    0x2,    0x2,    0x2,    0x6b0,  0x6b1,  0x3,
      0x2,   0x2,    0x2,    0x6b1,  0x6b4,  0x3,    0x2,    0x2,    0x2,
      0x6b2, 0x6b0,  0x3,    0x2,    0x2,    0x2,    0x6b3,  0x6ab,  0x3,
      0x2,   0x2,    0x2,    0x6b3,  0x6b4,  0x3,    0x2,    0x2,    0x2,
      0x6b4, 0x6b5,  0x3,    0x2,    0x2,    0x2,    0x6b5,  0x6b7,  0x7,
      0x5,   0x2,    0x2,    0x6b6,  0x5c7,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x5c9,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x5ca,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x5cd,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x5cf,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x5d0,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x5d1,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x5d2,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x5d3,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x5d4,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x5db,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x5e5,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x5f1,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x5fb,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x620,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x624,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x632,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x636,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x63b,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x648,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x654,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x65b,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x662,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x66f,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x670,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x671,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x677,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x67d,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x683,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x689,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x68a,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x695,  0x3,    0x2,    0x2,    0x2,    0x6b6,  0x69e,  0x3,
      0x2,   0x2,    0x2,    0x6b6,  0x6a5,  0x3,    0x2,    0x2,    0x2,
      0x6b6, 0x6a9,  0x3,    0x2,    0x2,    0x2,    0x6b7,  0x6c2,  0x3,
      0x2,   0x2,    0x2,    0x6b8,  0x6b9,  0xc,    0x10,   0x2,    0x2,
      0x6b9, 0x6ba,  0x7,    0x9,    0x2,    0x2,    0x6ba,  0x6bb,  0x5,
      0x5c,  0x2f,   0x2,    0x6bb,  0x6bc,  0x7,    0xa,    0x2,    0x2,
      0x6bc, 0x6c1,  0x3,    0x2,    0x2,    0x2,    0x6bd,  0x6be,  0xc,
      0xe,   0x2,    0x2,    0x6be,  0x6bf,  0x7,    0x3,    0x2,    0x2,
      0x6bf, 0x6c1,  0x5,    0x9c,   0x4f,   0x2,    0x6c0,  0x6b8,  0x3,
      0x2,   0x2,    0x2,    0x6c0,  0x6bd,  0x3,    0x2,    0x2,    0x2,
      0x6c1, 0x6c4,  0x3,    0x2,    0x2,    0x2,    0x6c2,  0x6c0,  0x3,
      0x2,   0x2,    0x2,    0x6c2,  0x6c3,  0x3,    0x2,    0x2,    0x2,
      0x6c3, 0x5f,   0x3,    0x2,    0x2,    0x2,    0x6c4,  0x6c2,  0x3,
      0x2,   0x2,    0x2,    0x6c5,  0x6cc,  0x7,    0xf4,   0x2,    0x2,
      0x6c6, 0x6c9,  0x7,    0xf5,   0x2,    0x2,    0x6c7,  0x6c8,  0x7,
      0xd2,  0x2,    0x2,    0x6c8,  0x6ca,  0x7,    0xf4,   0x2,    0x2,
      0x6c9, 0x6c7,  0x3,    0x2,    0x2,    0x2,    0x6c9,  0x6ca,  0x3,
      0x2,   0x2,    0x2,    0x6ca,  0x6cc,  0x3,    0x2,    0x2,    0x2,
      0x6cb, 0x6c5,  0x3,    0x2,    0x2,    0x2,    0x6cb,  0x6c6,  0x3,
      0x2,   0x2,    0x2,    0x6cc,  0x61,   0x3,    0x2,    0x2,    0x2,
      0x6cd, 0x6ce,  0x7,    0x5f,   0x2,    0x2,    0x6ce,  0x6d2,  0x7,
      0x89,  0x2,    0x2,    0x6cf,  0x6d0,  0x7,    0xa6,   0x2,    0x2,
      0x6d0, 0x6d2,  0x7,    0x89,   0x2,    0x2,    0x6d1,  0x6cd,  0x3,
      0x2,   0x2,    0x2,    0x6d1,  0x6cf,  0x3,    0x2,    0x2,    0x2,
      0x6d2, 0x63,   0x3,    0x2,    0x2,    0x2,    0x6d3,  0x6d4,  0x7,
      0xca,  0x2,    0x2,    0x6d4,  0x6d5,  0x7,    0xe7,   0x2,    0x2,
      0x6d5, 0x6da,  0x5,    0x6c,   0x37,   0x2,    0x6d6,  0x6d7,  0x7,
      0xca,  0x2,    0x2,    0x6d7,  0x6d8,  0x7,    0xe7,   0x2,    0x2,
      0x6d8, 0x6da,  0x5,    0x60,   0x31,   0x2,    0x6d9,  0x6d3,  0x3,
      0x2,   0x2,    0x2,    0x6d9,  0x6d6,  0x3,    0x2,    0x2,    0x2,
      0x6da, 0x65,   0x3,    0x2,    0x2,    0x2,    0x6db,  0x6dc,  0x9,
      0xf,   0x2,    0x2,    0x6dc,  0x67,   0x3,    0x2,    0x2,    0x2,
      0x6dd, 0x6de,  0x9,    0x10,   0x2,    0x2,    0x6de,  0x69,   0x3,
      0x2,   0x2,    0x2,    0x6df,  0x6e0,  0x9,    0x11,   0x2,    0x2,
      0x6e0, 0x6b,   0x3,    0x2,    0x2,    0x2,    0x6e1,  0x6e3,  0x7,
      0x66,  0x2,    0x2,    0x6e2,  0x6e4,  0x9,    0xd,    0x2,    0x2,
      0x6e3, 0x6e2,  0x3,    0x2,    0x2,    0x2,    0x6e3,  0x6e4,  0x3,
      0x2,   0x2,    0x2,    0x6e4,  0x6e5,  0x3,    0x2,    0x2,    0x2,
      0x6e5, 0x6e6,  0x5,    0x60,   0x31,   0x2,    0x6e6,  0x6e9,  0x5,
      0x6e,  0x38,   0x2,    0x6e7,  0x6e8,  0x7,    0xcc,   0x2,    0x2,
      0x6e8, 0x6ea,  0x5,    0x6e,   0x38,   0x2,    0x6e9,  0x6e7,  0x3,
      0x2,   0x2,    0x2,    0x6e9,  0x6ea,  0x3,    0x2,    0x2,    0x2,
      0x6ea, 0x6d,   0x3,    0x2,    0x2,    0x2,    0x6eb,  0x6ec,  0x9,
      0x12,  0x2,    0x2,    0x6ec,  0x6f,   0x3,    0x2,    0x2,    0x2,
      0x6ed, 0x6ee,  0x9,    0x13,   0x2,    0x2,    0x6ee,  0x71,   0x3,
      0x2,   0x2,    0x2,    0x6ef,  0x6f8,  0x7,    0x4,    0x2,    0x2,
      0x6f0, 0x6f5,  0x5,    0x74,   0x3b,   0x2,    0x6f1,  0x6f2,  0x7,
      0x6,   0x2,    0x2,    0x6f2,  0x6f4,  0x5,    0x74,   0x3b,   0x2,
      0x6f3, 0x6f1,  0x3,    0x2,    0x2,    0x2,    0x6f4,  0x6f7,  0x3,
      0x2,   0x2,    0x2,    0x6f5,  0x6f3,  0x3,    0x2,    0x2,    0x2,
      0x6f5, 0x6f6,  0x3,    0x2,    0x2,    0x2,    0x6f6,  0x6f9,  0x3,
      0x2,   0x2,    0x2,    0x6f7,  0x6f5,  0x3,    0x2,    0x2,    0x2,
      0x6f8, 0x6f0,  0x3,    0x2,    0x2,    0x2,    0x6f8,  0x6f9,  0x3,
      0x2,   0x2,    0x2,    0x6f9,  0x6fa,  0x3,    0x2,    0x2,    0x2,
      0x6fa, 0x6fb,  0x7,    0x5,    0x2,    0x2,    0x6fb,  0x73,   0x3,
      0x2,   0x2,    0x2,    0x6fc,  0x6fd,  0x8,    0x3b,   0x1,    0x2,
      0x6fd, 0x6fe,  0x7,    0x14,   0x2,    0x2,    0x6fe,  0x6ff,  0x7,
      0xea,  0x2,    0x2,    0x6ff,  0x700,  0x5,    0x74,   0x3b,   0x2,
      0x700, 0x701,  0x7,    0xec,   0x2,    0x2,    0x701,  0x72c,  0x3,
      0x2,   0x2,    0x2,    0x702,  0x703,  0x7,    0x79,   0x2,    0x2,
      0x703, 0x704,  0x7,    0xea,   0x2,    0x2,    0x704,  0x705,  0x5,
      0x74,  0x3b,   0x2,    0x705,  0x706,  0x7,    0x6,    0x2,    0x2,
      0x706, 0x707,  0x5,    0x74,   0x3b,   0x2,    0x707,  0x708,  0x7,
      0xec,  0x2,    0x2,    0x708,  0x72c,  0x3,    0x2,    0x2,    0x2,
      0x709, 0x70a,  0x7,    0xb0,   0x2,    0x2,    0x70a,  0x70b,  0x7,
      0x4,   0x2,    0x2,    0x70b,  0x70c,  0x5,    0x9c,   0x4f,   0x2,
      0x70c, 0x713,  0x5,    0x74,   0x3b,   0x2,    0x70d,  0x70e,  0x7,
      0x6,   0x2,    0x2,    0x70e,  0x70f,  0x5,    0x9c,   0x4f,   0x2,
      0x70f, 0x710,  0x5,    0x74,   0x3b,   0x2,    0x710,  0x712,  0x3,
      0x2,   0x2,    0x2,    0x711,  0x70d,  0x3,    0x2,    0x2,    0x2,
      0x712, 0x715,  0x3,    0x2,    0x2,    0x2,    0x713,  0x711,  0x3,
      0x2,   0x2,    0x2,    0x713,  0x714,  0x3,    0x2,    0x2,    0x2,
      0x714, 0x716,  0x3,    0x2,    0x2,    0x2,    0x715,  0x713,  0x3,
      0x2,   0x2,    0x2,    0x716,  0x717,  0x7,    0x5,    0x2,    0x2,
      0x717, 0x72c,  0x3,    0x2,    0x2,    0x2,    0x718,  0x724,  0x5,
      0x78,  0x3d,   0x2,    0x719,  0x71a,  0x7,    0x4,    0x2,    0x2,
      0x71a, 0x71f,  0x5,    0x76,   0x3c,   0x2,    0x71b,  0x71c,  0x7,
      0x6,   0x2,    0x2,    0x71c,  0x71e,  0x5,    0x76,   0x3c,   0x2,
      0x71d, 0x71b,  0x3,    0x2,    0x2,    0x2,    0x71e,  0x721,  0x3,
      0x2,   0x2,    0x2,    0x71f,  0x71d,  0x3,    0x2,    0x2,    0x2,
      0x71f, 0x720,  0x3,    0x2,    0x2,    0x2,    0x720,  0x722,  0x3,
      0x2,   0x2,    0x2,    0x721,  0x71f,  0x3,    0x2,    0x2,    0x2,
      0x722, 0x723,  0x7,    0x5,    0x2,    0x2,    0x723,  0x725,  0x3,
      0x2,   0x2,    0x2,    0x724,  0x719,  0x3,    0x2,    0x2,    0x2,
      0x724, 0x725,  0x3,    0x2,    0x2,    0x2,    0x725,  0x72c,  0x3,
      0x2,   0x2,    0x2,    0x726,  0x727,  0x7,    0x66,   0x2,    0x2,
      0x727, 0x728,  0x5,    0x6e,   0x38,   0x2,    0x728,  0x729,  0x7,
      0xcc,  0x2,    0x2,    0x729,  0x72a,  0x5,    0x6e,   0x38,   0x2,
      0x72a, 0x72c,  0x3,    0x2,    0x2,    0x2,    0x72b,  0x6fc,  0x3,
      0x2,   0x2,    0x2,    0x72b,  0x702,  0x3,    0x2,    0x2,    0x2,
      0x72b, 0x709,  0x3,    0x2,    0x2,    0x2,    0x72b,  0x718,  0x3,
      0x2,   0x2,    0x2,    0x72b,  0x726,  0x3,    0x2,    0x2,    0x2,
      0x72c, 0x731,  0x3,    0x2,    0x2,    0x2,    0x72d,  0x72e,  0xc,
      0x8,   0x2,    0x2,    0x72e,  0x730,  0x7,    0x14,   0x2,    0x2,
      0x72f, 0x72d,  0x3,    0x2,    0x2,    0x2,    0x730,  0x733,  0x3,
      0x2,   0x2,    0x2,    0x731,  0x72f,  0x3,    0x2,    0x2,    0x2,
      0x731, 0x732,  0x3,    0x2,    0x2,    0x2,    0x732,  0x75,   0x3,
      0x2,   0x2,    0x2,    0x733,  0x731,  0x3,    0x2,    0x2,    0x2,
      0x734, 0x737,  0x7,    0xf7,   0x2,    0x2,    0x735,  0x737,  0x5,
      0x74,  0x3b,   0x2,    0x736,  0x734,  0x3,    0x2,    0x2,    0x2,
      0x736, 0x735,  0x3,    0x2,    0x2,    0x2,    0x737,  0x77,   0x3,
      0x2,   0x2,    0x2,    0x738,  0x73d,  0x7,    0xfe,   0x2,    0x2,
      0x739, 0x73d,  0x7,    0xff,   0x2,    0x2,    0x73a,  0x73d,  0x7,
      0x100, 0x2,    0x2,    0x73b,  0x73d,  0x5,    0x90,   0x49,   0x2,
      0x73c, 0x738,  0x3,    0x2,    0x2,    0x2,    0x73c,  0x739,  0x3,
      0x2,   0x2,    0x2,    0x73c,  0x73a,  0x3,    0x2,    0x2,    0x2,
      0x73c, 0x73b,  0x3,    0x2,    0x2,    0x2,    0x73d,  0x79,   0x3,
      0x2,   0x2,    0x2,    0x73e,  0x73f,  0x7,    0xe1,   0x2,    0x2,
      0x73f, 0x740,  0x5,    0x56,   0x2c,   0x2,    0x740,  0x741,  0x7,
      0xc9,  0x2,    0x2,    0x741,  0x742,  0x5,    0x56,   0x2c,   0x2,
      0x742, 0x7b,   0x3,    0x2,    0x2,    0x2,    0x743,  0x744,  0x7,
      0x4c,  0x2,    0x2,    0x744,  0x745,  0x7,    0x4,    0x2,    0x2,
      0x745, 0x746,  0x7,    0xe2,   0x2,    0x2,    0x746,  0x747,  0x5,
      0x58,  0x2d,   0x2,    0x747,  0x748,  0x7,    0x5,    0x2,    0x2,
      0x748, 0x7d,   0x3,    0x2,    0x2,    0x2,    0x749,  0x74a,  0x7,
      0x94,  0x2,    0x2,    0x74a,  0x755,  0x7,    0x4,    0x2,    0x2,
      0x74b, 0x74c,  0x7,    0x95,   0x2,    0x2,    0x74c,  0x74d,  0x7,
      0x1b,  0x2,    0x2,    0x74d,  0x752,  0x5,    0x56,   0x2c,   0x2,
      0x74e, 0x74f,  0x7,    0x6,    0x2,    0x2,    0x74f,  0x751,  0x5,
      0x56,  0x2c,   0x2,    0x750,  0x74e,  0x3,    0x2,    0x2,    0x2,
      0x751, 0x754,  0x3,    0x2,    0x2,    0x2,    0x752,  0x750,  0x3,
      0x2,   0x2,    0x2,    0x752,  0x753,  0x3,    0x2,    0x2,    0x2,
      0x753, 0x756,  0x3,    0x2,    0x2,    0x2,    0x754,  0x752,  0x3,
      0x2,   0x2,    0x2,    0x755,  0x74b,  0x3,    0x2,    0x2,    0x2,
      0x755, 0x756,  0x3,    0x2,    0x2,    0x2,    0x756,  0x761,  0x3,
      0x2,   0x2,    0x2,    0x757,  0x758,  0x7,    0x90,   0x2,    0x2,
      0x758, 0x759,  0x7,    0x1b,   0x2,    0x2,    0x759,  0x75e,  0x5,
      0x36,  0x1c,   0x2,    0x75a,  0x75b,  0x7,    0x6,    0x2,    0x2,
      0x75b, 0x75d,  0x5,    0x36,   0x1c,   0x2,    0x75c,  0x75a,  0x3,
      0x2,   0x2,    0x2,    0x75d,  0x760,  0x3,    0x2,    0x2,    0x2,
      0x75e, 0x75c,  0x3,    0x2,    0x2,    0x2,    0x75e,  0x75f,  0x3,
      0x2,   0x2,    0x2,    0x75f,  0x762,  0x3,    0x2,    0x2,    0x2,
      0x760, 0x75e,  0x3,    0x2,    0x2,    0x2,    0x761,  0x757,  0x3,
      0x2,   0x2,    0x2,    0x761,  0x762,  0x3,    0x2,    0x2,    0x2,
      0x762, 0x764,  0x3,    0x2,    0x2,    0x2,    0x763,  0x765,  0x5,
      0x80,  0x41,   0x2,    0x764,  0x763,  0x3,    0x2,    0x2,    0x2,
      0x764, 0x765,  0x3,    0x2,    0x2,    0x2,    0x765,  0x766,  0x3,
      0x2,   0x2,    0x2,    0x766,  0x767,  0x7,    0x5,    0x2,    0x2,
      0x767, 0x7f,   0x3,    0x2,    0x2,    0x2,    0x768,  0x769,  0x7,
      0x9d,  0x2,    0x2,    0x769,  0x781,  0x5,    0x82,   0x42,   0x2,
      0x76a, 0x76b,  0x7,    0xb1,   0x2,    0x2,    0x76b,  0x781,  0x5,
      0x82,  0x42,   0x2,    0x76c,  0x76d,  0x7,    0x5b,   0x2,    0x2,
      0x76d, 0x781,  0x5,    0x82,   0x42,   0x2,    0x76e,  0x76f,  0x7,
      0x9d,  0x2,    0x2,    0x76f,  0x770,  0x7,    0x1a,   0x2,    0x2,
      0x770, 0x771,  0x5,    0x82,   0x42,   0x2,    0x771,  0x772,  0x7,
      0x12,  0x2,    0x2,    0x772,  0x773,  0x5,    0x82,   0x42,   0x2,
      0x773, 0x781,  0x3,    0x2,    0x2,    0x2,    0x774,  0x775,  0x7,
      0xb1,  0x2,    0x2,    0x775,  0x776,  0x7,    0x1a,   0x2,    0x2,
      0x776, 0x777,  0x5,    0x82,   0x42,   0x2,    0x777,  0x778,  0x7,
      0x12,  0x2,    0x2,    0x778,  0x779,  0x5,    0x82,   0x42,   0x2,
      0x779, 0x781,  0x3,    0x2,    0x2,    0x2,    0x77a,  0x77b,  0x7,
      0x5b,  0x2,    0x2,    0x77b,  0x77c,  0x7,    0x1a,   0x2,    0x2,
      0x77c, 0x77d,  0x5,    0x82,   0x42,   0x2,    0x77d,  0x77e,  0x7,
      0x12,  0x2,    0x2,    0x77e,  0x77f,  0x5,    0x82,   0x42,   0x2,
      0x77f, 0x781,  0x3,    0x2,    0x2,    0x2,    0x780,  0x768,  0x3,
      0x2,   0x2,    0x2,    0x780,  0x76a,  0x3,    0x2,    0x2,    0x2,
      0x780, 0x76c,  0x3,    0x2,    0x2,    0x2,    0x780,  0x76e,  0x3,
      0x2,   0x2,    0x2,    0x780,  0x774,  0x3,    0x2,    0x2,    0x2,
      0x780, 0x77a,  0x3,    0x2,    0x2,    0x2,    0x781,  0x81,   0x3,
      0x2,   0x2,    0x2,    0x782,  0x783,  0x7,    0xd3,   0x2,    0x2,
      0x783, 0x78c,  0x7,    0x98,   0x2,    0x2,    0x784,  0x785,  0x7,
      0xd3,  0x2,    0x2,    0x785,  0x78c,  0x7,    0x4e,   0x2,    0x2,
      0x786, 0x787,  0x7,    0x2b,   0x2,    0x2,    0x787,  0x78c,  0x7,
      0xb0,  0x2,    0x2,    0x788,  0x789,  0x5,    0x56,   0x2c,   0x2,
      0x789, 0x78a,  0x9,    0x14,   0x2,    0x2,    0x78a,  0x78c,  0x3,
      0x2,   0x2,    0x2,    0x78b,  0x782,  0x3,    0x2,    0x2,    0x2,
      0x78b, 0x784,  0x3,    0x2,    0x2,    0x2,    0x78b,  0x786,  0x3,
      0x2,   0x2,    0x2,    0x78b,  0x788,  0x3,    0x2,    0x2,    0x2,
      0x78c, 0x83,   0x3,    0x2,    0x2,    0x2,    0x78d,  0x78e,  0x5,
      0x9c,  0x4f,   0x2,    0x78e,  0x78f,  0x7,    0xe8,   0x2,    0x2,
      0x78f, 0x790,  0x5,    0x56,   0x2c,   0x2,    0x790,  0x85,   0x3,
      0x2,   0x2,    0x2,    0x791,  0x792,  0x7,    0x50,   0x2,    0x2,
      0x792, 0x796,  0x9,    0x15,   0x2,    0x2,    0x793,  0x794,  0x7,
      0xd1,  0x2,    0x2,    0x794,  0x796,  0x9,    0x16,   0x2,    0x2,
      0x795, 0x791,  0x3,    0x2,    0x2,    0x2,    0x795,  0x793,  0x3,
      0x2,   0x2,    0x2,    0x796,  0x87,   0x3,    0x2,    0x2,    0x2,
      0x797, 0x798,  0x7,    0x6b,   0x2,    0x2,    0x798,  0x799,  0x7,
      0x73,  0x2,    0x2,    0x799,  0x79d,  0x5,    0x8a,   0x46,   0x2,
      0x79a, 0x79b,  0x7,    0x9e,   0x2,    0x2,    0x79b,  0x79d,  0x9,
      0x17,  0x2,    0x2,    0x79c,  0x797,  0x3,    0x2,    0x2,    0x2,
      0x79c, 0x79a,  0x3,    0x2,    0x2,    0x2,    0x79d,  0x89,   0x3,
      0x2,   0x2,    0x2,    0x79e,  0x79f,  0x7,    0x9e,   0x2,    0x2,
      0x79f, 0x7a6,  0x7,    0xd4,   0x2,    0x2,    0x7a0,  0x7a1,  0x7,
      0x9e,  0x2,    0x2,    0x7a1,  0x7a6,  0x7,    0x26,   0x2,    0x2,
      0x7a2, 0x7a3,  0x7,    0xa3,   0x2,    0x2,    0x7a3,  0x7a6,  0x7,
      0x9e,  0x2,    0x2,    0x7a4,  0x7a6,  0x7,    0xb7,   0x2,    0x2,
      0x7a5, 0x79e,  0x3,    0x2,    0x2,    0x2,    0x7a5,  0x7a0,  0x3,
      0x2,   0x2,    0x2,    0x7a5,  0x7a2,  0x3,    0x2,    0x2,    0x2,
      0x7a5, 0x7a4,  0x3,    0x2,    0x2,    0x2,    0x7a6,  0x8b,   0x3,
      0x2,   0x2,    0x2,    0x7a7,  0x7ad,  0x5,    0x56,   0x2c,   0x2,
      0x7a8, 0x7a9,  0x5,    0x9c,   0x4f,   0x2,    0x7a9,  0x7aa,  0x7,
      0xb,   0x2,    0x2,    0x7aa,  0x7ab,  0x5,    0x56,   0x2c,   0x2,
      0x7ab, 0x7ad,  0x3,    0x2,    0x2,    0x2,    0x7ac,  0x7a7,  0x3,
      0x2,   0x2,    0x2,    0x7ac,  0x7a8,  0x3,    0x2,    0x2,    0x2,
      0x7ad, 0x8d,   0x3,    0x2,    0x2,    0x2,    0x7ae,  0x7b3,  0x7,
      0xb6,  0x2,    0x2,    0x7af,  0x7b3,  0x7,    0x36,   0x2,    0x2,
      0x7b0, 0x7b3,  0x7,    0x64,   0x2,    0x2,    0x7b1,  0x7b3,  0x5,
      0x9c,  0x4f,   0x2,    0x7b2,  0x7ae,  0x3,    0x2,    0x2,    0x2,
      0x7b2, 0x7af,  0x3,    0x2,    0x2,    0x2,    0x7b2,  0x7b0,  0x3,
      0x2,   0x2,    0x2,    0x7b2,  0x7b1,  0x3,    0x2,    0x2,    0x2,
      0x7b3, 0x8f,   0x3,    0x2,    0x2,    0x2,    0x7b4,  0x7b9,  0x5,
      0x9c,  0x4f,   0x2,    0x7b5,  0x7b6,  0x7,    0x3,    0x2,    0x2,
      0x7b6, 0x7b8,  0x5,    0x9c,   0x4f,   0x2,    0x7b7,  0x7b5,  0x3,
      0x2,   0x2,    0x2,    0x7b8,  0x7bb,  0x3,    0x2,    0x2,    0x2,
      0x7b9, 0x7b7,  0x3,    0x2,    0x2,    0x2,    0x7b9,  0x7ba,  0x3,
      0x2,   0x2,    0x2,    0x7ba,  0x91,   0x3,    0x2,    0x2,    0x2,
      0x7bb, 0x7b9,  0x3,    0x2,    0x2,    0x2,    0x7bc,  0x7bd,  0x7,
      0x4f,  0x2,    0x2,    0x7bd,  0x7be,  0x9,    0x18,   0x2,    0x2,
      0x7be, 0x7bf,  0x5,    0x94,   0x4b,   0x2,    0x7bf,  0x7c0,  0x5,
      0x5c,  0x2f,   0x2,    0x7c0,  0x93,   0x3,    0x2,    0x2,    0x2,
      0x7c1, 0x7c2,  0x7,    0x15,   0x2,    0x2,    0x7c2,  0x7c5,  0x7,
      0x8a,  0x2,    0x2,    0x7c3,  0x7c5,  0x7,    0x18,   0x2,    0x2,
      0x7c4, 0x7c1,  0x3,    0x2,    0x2,    0x2,    0x7c4,  0x7c3,  0x3,
      0x2,   0x2,    0x2,    0x7c5,  0x95,   0x3,    0x2,    0x2,    0x2,
      0x7c6, 0x7ca,  0x7,    0x30,   0x2,    0x2,    0x7c7,  0x7ca,  0x7,
      0x2d,  0x2,    0x2,    0x7c8,  0x7ca,  0x5,    0x98,   0x4d,   0x2,
      0x7c9, 0x7c6,  0x3,    0x2,    0x2,    0x2,    0x7c9,  0x7c7,  0x3,
      0x2,   0x2,    0x2,    0x7c9,  0x7c8,  0x3,    0x2,    0x2,    0x2,
      0x7ca, 0x97,   0x3,    0x2,    0x2,    0x2,    0x7cb,  0x7cc,  0x7,
      0xda,  0x2,    0x2,    0x7cc,  0x7d1,  0x5,    0x9c,   0x4f,   0x2,
      0x7cd, 0x7ce,  0x7,    0xac,   0x2,    0x2,    0x7ce,  0x7d1,  0x5,
      0x9c,  0x4f,   0x2,    0x7cf,  0x7d1,  0x5,    0x9c,   0x4f,   0x2,
      0x7d0, 0x7cb,  0x3,    0x2,    0x2,    0x2,    0x7d0,  0x7cd,  0x3,
      0x2,   0x2,    0x2,    0x7d0,  0x7cf,  0x3,    0x2,    0x2,    0x2,
      0x7d1, 0x99,   0x3,    0x2,    0x2,    0x2,    0x7d2,  0x7d7,  0x5,
      0x9c,  0x4f,   0x2,    0x7d3,  0x7d4,  0x7,    0x6,    0x2,    0x2,
      0x7d4, 0x7d6,  0x5,    0x9c,   0x4f,   0x2,    0x7d5,  0x7d3,  0x3,
      0x2,   0x2,    0x2,    0x7d6,  0x7d9,  0x3,    0x2,    0x2,    0x2,
      0x7d7, 0x7d5,  0x3,    0x2,    0x2,    0x2,    0x7d7,  0x7d8,  0x3,
      0x2,   0x2,    0x2,    0x7d8,  0x9b,   0x3,    0x2,    0x2,    0x2,
      0x7d9, 0x7d7,  0x3,    0x2,    0x2,    0x2,    0x7da,  0x7e0,  0x7,
      0xfa,  0x2,    0x2,    0x7db,  0x7e0,  0x7,    0xfc,   0x2,    0x2,
      0x7dc, 0x7e0,  0x5,    0xb2,   0x5a,   0x2,    0x7dd,  0x7e0,  0x7,
      0xfd,  0x2,    0x2,    0x7de,  0x7e0,  0x7,    0xfb,   0x2,    0x2,
      0x7df, 0x7da,  0x3,    0x2,    0x2,    0x2,    0x7df,  0x7db,  0x3,
      0x2,   0x2,    0x2,    0x7df,  0x7dc,  0x3,    0x2,    0x2,    0x2,
      0x7df, 0x7dd,  0x3,    0x2,    0x2,    0x2,    0x7df,  0x7de,  0x3,
      0x2,   0x2,    0x2,    0x7e0,  0x9d,   0x3,    0x2,    0x2,    0x2,
      0x7e1, 0x7e5,  0x7,    0xf8,   0x2,    0x2,    0x7e2,  0x7e5,  0x7,
      0xf9,  0x2,    0x2,    0x7e3,  0x7e5,  0x7,    0xf7,   0x2,    0x2,
      0x7e4, 0x7e1,  0x3,    0x2,    0x2,    0x2,    0x7e4,  0x7e2,  0x3,
      0x2,   0x2,    0x2,    0x7e4,  0x7e3,  0x3,    0x2,    0x2,    0x2,
      0x7e5, 0x9f,   0x3,    0x2,    0x2,    0x2,    0x7e6,  0x7e9,  0x5,
      0xa2,  0x52,   0x2,    0x7e7,  0x7e9,  0x5,    0xa4,   0x53,   0x2,
      0x7e8, 0x7e6,  0x3,    0x2,    0x2,    0x2,    0x7e8,  0x7e7,  0x3,
      0x2,   0x2,    0x2,    0x7e9,  0xa1,   0x3,    0x2,    0x2,    0x2,
      0x7ea, 0x7eb,  0x7,    0x27,   0x2,    0x2,    0x7eb,  0x7ec,  0x5,
      0x9c,  0x4f,   0x2,    0x7ec,  0x7ed,  0x5,    0xa4,   0x53,   0x2,
      0x7ed, 0xa3,   0x3,    0x2,    0x2,    0x2,    0x7ee,  0x7ef,  0x5,
      0xa6,  0x54,   0x2,    0x7ef,  0x7f1,  0x5,    0x52,   0x2a,   0x2,
      0x7f0, 0x7f2,  0x5,    0xa8,   0x55,   0x2,    0x7f1,  0x7f0,  0x3,
      0x2,   0x2,    0x2,    0x7f1,  0x7f2,  0x3,    0x2,    0x2,    0x2,
      0x7f2, 0xa5,   0x3,    0x2,    0x2,    0x2,    0x7f3,  0x7f7,  0x7,
      0xd6,  0x2,    0x2,    0x7f4,  0x7f5,  0x7,    0x9a,   0x2,    0x2,
      0x7f5, 0x7f7,  0x7,    0x6e,   0x2,    0x2,    0x7f6,  0x7f3,  0x3,
      0x2,   0x2,    0x2,    0x7f6,  0x7f4,  0x3,    0x2,    0x2,    0x2,
      0x7f7, 0xa7,   0x3,    0x2,    0x2,    0x2,    0x7f8,  0x7fa,  0x5,
      0xaa,  0x56,   0x2,    0x7f9,  0x7f8,  0x3,    0x2,    0x2,    0x2,
      0x7fa, 0x7fd,  0x3,    0x2,    0x2,    0x2,    0x7fb,  0x7f9,  0x3,
      0x2,   0x2,    0x2,    0x7fb,  0x7fc,  0x3,    0x2,    0x2,    0x2,
      0x7fc, 0xa9,   0x3,    0x2,    0x2,    0x2,    0x7fd,  0x7fb,  0x3,
      0x2,   0x2,    0x2,    0x7fe,  0x802,  0x5,    0xae,   0x58,   0x2,
      0x7ff, 0x802,  0x5,    0xac,   0x57,   0x2,    0x800,  0x802,  0x5,
      0xb0,  0x59,   0x2,    0x801,  0x7fe,  0x3,    0x2,    0x2,    0x2,
      0x801, 0x7ff,  0x3,    0x2,    0x2,    0x2,    0x801,  0x800,  0x3,
      0x2,   0x2,    0x2,    0x802,  0xab,   0x3,    0x2,    0x2,    0x2,
      0x803, 0x807,  0x7,    0xa1,   0x2,    0x2,    0x804,  0x805,  0x7,
      0x86,  0x2,    0x2,    0x805,  0x807,  0x7,    0xa1,   0x2,    0x2,
      0x806, 0x803,  0x3,    0x2,    0x2,    0x2,    0x806,  0x804,  0x3,
      0x2,   0x2,    0x2,    0x807,  0xad,   0x3,    0x2,    0x2,    0x2,
      0x808, 0x809,  0x9,    0x19,   0x2,    0x2,    0x809,  0xaf,   0x3,
      0x2,   0x2,    0x2,    0x80a,  0x80e,  0x7,    0x41,   0x2,    0x2,
      0x80b, 0x80c,  0x7,    0x86,   0x2,    0x2,    0x80c,  0x80e,  0x7,
      0x41,  0x2,    0x2,    0x80d,  0x80a,  0x3,    0x2,    0x2,    0x2,
      0x80d, 0x80b,  0x3,    0x2,    0x2,    0x2,    0x80e,  0xb1,   0x3,
      0x2,   0x2,    0x2,    0x80f,  0x810,  0x9,    0x1a,   0x2,    0x2,
      0x810, 0xb3,   0x3,    0x2,    0x2,    0x2,    0x108,  0xca,   0xcf,
      0xd5,  0xd9,   0xe7,   0xeb,   0xef,   0xf3,   0xfb,   0xff,   0x102,
      0x109, 0x112,  0x118,  0x11c,  0x122,  0x129,  0x132,  0x13b,  0x146,
      0x14d, 0x157,  0x15e,  0x166,  0x16e,  0x176,  0x180,  0x187,  0x18f,
      0x194, 0x19f,  0x1a4,  0x1af,  0x1ba,  0x1c6,  0x1cc,  0x1d1,  0x1d7,
      0x1e0, 0x1eb,  0x1f4,  0x1f9,  0x1fd,  0x205,  0x20c,  0x219,  0x21c,
      0x226, 0x229,  0x230,  0x239,  0x23f,  0x244,  0x248,  0x252,  0x255,
      0x25f, 0x26c,  0x272,  0x277,  0x27d,  0x286,  0x28c,  0x293,  0x29b,
      0x2a0, 0x2a4,  0x2ac,  0x2b2,  0x2b9,  0x2be,  0x2c2,  0x2cc,  0x2cf,
      0x2d3, 0x2d6,  0x2de,  0x2e3,  0x2f8,  0x2fe,  0x304,  0x306,  0x30c,
      0x312, 0x314,  0x31c,  0x31e,  0x331,  0x336,  0x33d,  0x349,  0x34b,
      0x353, 0x355,  0x367,  0x36a,  0x36e,  0x372,  0x384,  0x387,  0x397,
      0x39c, 0x39e,  0x3a1,  0x3a7,  0x3ae,  0x3b4,  0x3ba,  0x3be,  0x3c2,
      0x3c8, 0x3d0,  0x3df,  0x3e6,  0x3eb,  0x3f2,  0x3fa,  0x3fe,  0x403,
      0x40e, 0x41a,  0x41d,  0x422,  0x424,  0x42d,  0x42f,  0x437,  0x43d,
      0x440, 0x442,  0x44e,  0x455,  0x459,  0x45d,  0x461,  0x468,  0x471,
      0x474, 0x478,  0x47d,  0x481,  0x484,  0x48b,  0x496,  0x499,  0x4a3,
      0x4a6, 0x4b1,  0x4b6,  0x4be,  0x4c1,  0x4c5,  0x4c9,  0x4d4,  0x4d7,
      0x4de, 0x4f1,  0x4f5,  0x4f9,  0x4fd,  0x501,  0x505,  0x507,  0x512,
      0x517, 0x520,  0x526,  0x52a,  0x52c,  0x534,  0x53b,  0x548,  0x54e,
      0x559, 0x560,  0x564,  0x56c,  0x56e,  0x57b,  0x583,  0x58c,  0x592,
      0x59a, 0x5a0,  0x5a4,  0x5a9,  0x5ae,  0x5b4,  0x5c2,  0x5c4,  0x5e1,
      0x5ec, 0x5f6,  0x5f9,  0x5fe,  0x605,  0x608,  0x611,  0x614,  0x618,
      0x61b, 0x61e,  0x62a,  0x62d,  0x640,  0x644,  0x64c,  0x650,  0x669,
      0x66c, 0x675,  0x67b,  0x681,  0x687,  0x691,  0x69a,  0x6b0,  0x6b3,
      0x6b6, 0x6c0,  0x6c2,  0x6c9,  0x6cb,  0x6d1,  0x6d9,  0x6e3,  0x6e9,
      0x6f5, 0x6f8,  0x713,  0x71f,  0x724,  0x72b,  0x731,  0x736,  0x73c,
      0x752, 0x755,  0x75e,  0x761,  0x764,  0x780,  0x78b,  0x795,  0x79c,
      0x7a5, 0x7ac,  0x7b2,  0x7b9,  0x7c4,  0x7c9,  0x7d0,  0x7d7,  0x7df,
      0x7e4, 0x7e8,  0x7f1,  0x7f6,  0x7fb,  0x801,  0x806,  0x80d,
  };

  _serializedATN.insert(
      _serializedATN.end(),
      serializedATNSegment0,
      serializedATNSegment0 +
          sizeof(serializedATNSegment0) / sizeof(serializedATNSegment0[0]));

  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

PrestoSqlParser::Initializer PrestoSqlParser::_init;
