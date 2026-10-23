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

// Generated from PrestoSql.g4 by ANTLR 4.13.2

#pragma once

#include <stddef.h>
#include <string>
#include <vector>

#include <Parser.h>
#include <ParserRuleContext.h>
#include <Token.h>
#include <TokenStream.h>
#include <atn/ParserATNSimulatorOptions.h>
#include <tree/TerminalNode.h>

#include "antlr4-runtime.h"

class PrestoSqlParser : public antlr4::Parser {
 public:
  enum {
    T__0 = 1,
    T__1 = 2,
    T__2 = 3,
    T__3 = 4,
    T__4 = 5,
    T__5 = 6,
    T__6 = 7,
    T__7 = 8,
    T__8 = 9,
    T__9 = 10,
    T__10 = 11,
    T__11 = 12,
    T__12 = 13,
    T__13 = 14,
    ENGLISH_TOKEN = 15,
    ACYCLIC = 16,
    ADD = 17,
    ADMIN = 18,
    ALL = 19,
    ALTER = 20,
    ANALYZE = 21,
    AND = 22,
    ANY = 23,
    ARRAY = 24,
    AS = 25,
    ASC = 26,
    AT = 27,
    BEFORE = 28,
    BERNOULLI = 29,
    BETWEEN = 30,
    BY = 31,
    CALL = 32,
    CALLED = 33,
    CASCADE = 34,
    CASE = 35,
    CAST = 36,
    CATALOGS = 37,
    COLUMN = 38,
    COLUMNS = 39,
    COMMENT = 40,
    COMMIT = 41,
    COMMITTED = 42,
    CONSTRAINT = 43,
    CREATE = 44,
    CROSS = 45,
    CUBE = 46,
    CURRENT = 47,
    CURRENT_DATE = 48,
    CURRENT_ROLE = 49,
    CURRENT_TIME = 50,
    CURRENT_TIMESTAMP = 51,
    CURRENT_USER = 52,
    DATA = 53,
    DATE = 54,
    DAY = 55,
    DEALLOCATE = 56,
    DEFINER = 57,
    DELETE = 58,
    DESC = 59,
    DESCRIBE = 60,
    DESTINATION = 61,
    DETERMINISTIC = 62,
    DISABLED = 63,
    DISTINCT = 64,
    DISTRIBUTED = 65,
    DROP = 66,
    EDGE = 67,
    ELSE = 68,
    ENABLED = 69,
    END = 70,
    ENFORCED = 71,
    ESCAPE = 72,
    EXCEPT = 73,
    EXCLUDE = 74,
    EXCLUDING = 75,
    EXECUTABLE = 76,
    EXECUTE = 77,
    EXISTS = 78,
    EXPLAIN = 79,
    EXTRACT = 80,
    EXTERNAL = 81,
    FALSE = 82,
    FETCH = 83,
    FILTER = 84,
    FIRST = 85,
    FOLLOWING = 86,
    FOR = 87,
    FORMAT = 88,
    FROM = 89,
    FULL = 90,
    FUNCTION = 91,
    FUNCTIONS = 92,
    GRANT = 93,
    GRANTED = 94,
    GRANTS = 95,
    GRAPH = 96,
    GRAPH_TABLE = 97,
    GRAPHVIZ = 98,
    GROUP = 99,
    GROUPING = 100,
    GROUPS = 101,
    HAVING = 102,
    HOUR = 103,
    IF = 104,
    IGNORE = 105,
    IN = 106,
    INCLUDING = 107,
    INNER = 108,
    INPUT = 109,
    INSERT = 110,
    INTERSECT = 111,
    INTERVAL = 112,
    INTO = 113,
    INVOKER = 114,
    IO = 115,
    IS = 116,
    ISOLATION = 117,
    JSON = 118,
    JOIN = 119,
    KEY = 120,
    LABEL = 121,
    LANGUAGE = 122,
    LAST = 123,
    LATERAL = 124,
    LEFT = 125,
    LEVEL = 126,
    LIKE = 127,
    LIMIT = 128,
    LOCALTIME = 129,
    LOCALTIMESTAMP = 130,
    LOGICAL = 131,
    MAP = 132,
    MATCH = 133,
    MATERIALIZED = 134,
    MINUTE = 135,
    MONTH = 136,
    NAME = 137,
    NATURAL = 138,
    NFC = 139,
    NFD = 140,
    NFKC = 141,
    NFKD = 142,
    NO = 143,
    NONE = 144,
    NORMALIZE = 145,
    NOT = 146,
    NULL_LITERAL = 147,
    NULLIF = 148,
    NULLS = 149,
    OF = 150,
    OFFSET = 151,
    ON = 152,
    ONLY = 153,
    OPTIMIZED = 154,
    OPTION = 155,
    OR = 156,
    ORDER = 157,
    ORDINALITY = 158,
    OUTER = 159,
    OUTPUT = 160,
    OVER = 161,
    PARTITION = 162,
    PARTITIONS = 163,
    PATH = 164,
    PATHS = 165,
    POSITION = 166,
    PRECEDING = 167,
    PREPARE = 168,
    PRIMARY = 169,
    PRIVILEGES = 170,
    PROPERTIES = 171,
    RANGE = 172,
    READ = 173,
    RECURSIVE = 174,
    REFERENCES = 175,
    REFRESH = 176,
    RELY = 177,
    RENAME = 178,
    REPEATABLE = 179,
    REPLACE = 180,
    RESET = 181,
    RESPECT = 182,
    RESTRICT = 183,
    RETURN = 184,
    RETURNS = 185,
    REVOKE = 186,
    RIGHT = 187,
    ROLE = 188,
    ROLES = 189,
    ROLLBACK = 190,
    ROLLUP = 191,
    ROW = 192,
    ROWS = 193,
    SCHEMA = 194,
    SCHEMAS = 195,
    SECOND = 196,
    SECURITY = 197,
    SELECT = 198,
    SERIALIZABLE = 199,
    SESSION = 200,
    SET = 201,
    SETS = 202,
    SHORTEST = 203,
    SHOW = 204,
    SIMPLE = 205,
    SOME = 206,
    SOURCE = 207,
    SQL = 208,
    START = 209,
    STATS = 210,
    SUBSTRING = 211,
    SYSTEM = 212,
    SYSTEM_TIME = 213,
    SYSTEM_VERSION = 214,
    TABLE = 215,
    TABLES = 216,
    TABLESAMPLE = 217,
    TEMPORARY = 218,
    TEXT = 219,
    THEN = 220,
    TIME = 221,
    TIMESTAMP = 222,
    TO = 223,
    TRAIL = 224,
    TRANSACTION = 225,
    TRUE = 226,
    TRUNCATE = 227,
    TRY_CAST = 228,
    TYPE = 229,
    UESCAPE = 230,
    UNBOUNDED = 231,
    UNCOMMITTED = 232,
    UNION = 233,
    UNIQUE = 234,
    UNNEST = 235,
    UPDATE = 236,
    USE = 237,
    USER = 238,
    USING = 239,
    VALIDATE = 240,
    VALUES = 241,
    VERBOSE = 242,
    VERSION = 243,
    VERTEX = 244,
    VIEW = 245,
    WALK = 246,
    WHEN = 247,
    WHERE = 248,
    WITH = 249,
    WINDOW = 250,
    WORK = 251,
    WRITE = 252,
    YEAR = 253,
    ZONE = 254,
    EQ = 255,
    NEQ = 256,
    LT = 257,
    LTE = 258,
    GT = 259,
    GTE = 260,
    PLUS = 261,
    MINUS = 262,
    ASTERISK = 263,
    SLASH = 264,
    PERCENT = 265,
    CONCAT = 266,
    STRING = 267,
    UNICODE_STRING = 268,
    BINARY_LITERAL = 269,
    INTEGER_VALUE = 270,
    DECIMAL_VALUE = 271,
    DOUBLE_VALUE = 272,
    IDENTIFIER = 273,
    DIGIT_IDENTIFIER = 274,
    QUOTED_IDENTIFIER = 275,
    BACKQUOTED_IDENTIFIER = 276,
    TIME_WITH_TIME_ZONE = 277,
    TIMESTAMP_WITH_TIME_ZONE = 278,
    DOUBLE_PRECISION = 279,
    SIMPLE_COMMENT = 280,
    BRACKETED_COMMENT = 281,
    WS = 282,
    UNRECOGNIZED = 283,
    DELIMITER = 284
  };

  enum {
    RuleSingleStatement = 0,
    RuleStandaloneExpression = 1,
    RuleStandaloneRoutineBody = 2,
    RuleStatement = 3,
    RuleQuery = 4,
    RuleWith = 5,
    RuleWithElement = 6,
    RuleTableElement = 7,
    RuleColumnDefinition = 8,
    RuleLikeClause = 9,
    RuleProperties = 10,
    RuleProperty = 11,
    RuleSqlParameterDeclaration = 12,
    RuleRoutineCharacteristics = 13,
    RuleRoutineCharacteristic = 14,
    RuleAlterRoutineCharacteristics = 15,
    RuleAlterRoutineCharacteristic = 16,
    RuleRoutineBody = 17,
    RuleReturnStatement = 18,
    RuleExternalBodyReference = 19,
    RuleLanguage = 20,
    RuleDeterminism = 21,
    RuleNullCallClause = 22,
    RuleExternalRoutineName = 23,
    RuleQueryNoWith = 24,
    RuleQueryTerm = 25,
    RuleQueryPrimary = 26,
    RuleSortItem = 27,
    RuleQuerySpecification = 28,
    RuleWindowDefinition = 29,
    RuleGroupBy = 30,
    RuleGroupingElement = 31,
    RuleGroupingSet = 32,
    RuleNamedQuery = 33,
    RuleNamedGraph = 34,
    RuleVertexTable = 35,
    RuleEdgeTable = 36,
    RuleGraphElementKey = 37,
    RuleEdgeEndpoint = 38,
    RuleGraphElementLabels = 39,
    RuleGraphElementProperties = 40,
    RuleGraphElementProperty = 41,
    RuleSetQuantifier = 42,
    RuleSelectItem = 43,
    RuleStarModifiers = 44,
    RuleExcludeClause = 45,
    RuleReplaceClause = 46,
    RuleReplaceItem = 47,
    RuleRelation = 48,
    RuleJoinType = 49,
    RuleJoinCriteria = 50,
    RuleSampledRelation = 51,
    RuleSampleType = 52,
    RuleAliasedRelation = 53,
    RuleColumnAliases = 54,
    RuleRelationPrimary = 55,
    RuleGraphColumn = 56,
    RuleGraphPattern = 57,
    RulePathPattern = 58,
    RulePathPatternPrefix = 59,
    RulePathMode = 60,
    RulePathPatternExpression = 61,
    RulePathFactor = 62,
    RuleParenthesizedPathPattern = 63,
    RuleNodePattern = 64,
    RuleEdgePattern = 65,
    RuleElementPatternFiller = 66,
    RuleLabelExpression = 67,
    RuleLabelTerm = 68,
    RuleLabelFactor = 69,
    RuleLabelPrimary = 70,
    RuleGraphQuantifier = 71,
    RuleExpression = 72,
    RuleBooleanExpression = 73,
    RulePredicate = 74,
    RuleValueExpression = 75,
    RulePrimaryExpression = 76,
    RuleString = 77,
    RuleNullTreatment = 78,
    RuleTimeZoneSpecifier = 79,
    RuleComparisonOperator = 80,
    RuleComparisonQuantifier = 81,
    RuleBooleanValue = 82,
    RuleInterval = 83,
    RuleIntervalField = 84,
    RuleNormalForm = 85,
    RuleTypes = 86,
    RuleType = 87,
    RuleTypeParameter = 88,
    RuleBaseType = 89,
    RuleWhenClause = 90,
    RuleFilter = 91,
    RuleOver = 92,
    RuleWindowSpecification = 93,
    RuleWindowFrame = 94,
    RuleFrameBound = 95,
    RuleUpdateAssignment = 96,
    RuleExplainOption = 97,
    RuleTransactionMode = 98,
    RuleLevelOfIsolation = 99,
    RuleCallArgument = 100,
    RulePrivilege = 101,
    RuleQualifiedName = 102,
    RuleTableVersionExpression = 103,
    RuleTableVersionState = 104,
    RuleGrantor = 105,
    RulePrincipal = 106,
    RuleRoles = 107,
    RuleIdentifier = 108,
    RuleNumber = 109,
    RuleConstraintSpecification = 110,
    RuleNamedConstraintSpecification = 111,
    RuleUnnamedConstraintSpecification = 112,
    RuleConstraintType = 113,
    RuleConstraintQualifiers = 114,
    RuleConstraintQualifier = 115,
    RuleConstraintRely = 116,
    RuleConstraintEnabled = 117,
    RuleConstraintEnforced = 118,
    RuleNonReserved = 119
  };

  explicit PrestoSqlParser(antlr4::TokenStream* input);

  PrestoSqlParser(
      antlr4::TokenStream* input,
      const antlr4::atn::ParserATNSimulatorOptions& options);

  ~PrestoSqlParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  class SingleStatementContext;
  class StandaloneExpressionContext;
  class StandaloneRoutineBodyContext;
  class StatementContext;
  class QueryContext;
  class WithContext;
  class WithElementContext;
  class TableElementContext;
  class ColumnDefinitionContext;
  class LikeClauseContext;
  class PropertiesContext;
  class PropertyContext;
  class SqlParameterDeclarationContext;
  class RoutineCharacteristicsContext;
  class RoutineCharacteristicContext;
  class AlterRoutineCharacteristicsContext;
  class AlterRoutineCharacteristicContext;
  class RoutineBodyContext;
  class ReturnStatementContext;
  class ExternalBodyReferenceContext;
  class LanguageContext;
  class DeterminismContext;
  class NullCallClauseContext;
  class ExternalRoutineNameContext;
  class QueryNoWithContext;
  class QueryTermContext;
  class QueryPrimaryContext;
  class SortItemContext;
  class QuerySpecificationContext;
  class WindowDefinitionContext;
  class GroupByContext;
  class GroupingElementContext;
  class GroupingSetContext;
  class NamedQueryContext;
  class NamedGraphContext;
  class VertexTableContext;
  class EdgeTableContext;
  class GraphElementKeyContext;
  class EdgeEndpointContext;
  class GraphElementLabelsContext;
  class GraphElementPropertiesContext;
  class GraphElementPropertyContext;
  class SetQuantifierContext;
  class SelectItemContext;
  class StarModifiersContext;
  class ExcludeClauseContext;
  class ReplaceClauseContext;
  class ReplaceItemContext;
  class RelationContext;
  class JoinTypeContext;
  class JoinCriteriaContext;
  class SampledRelationContext;
  class SampleTypeContext;
  class AliasedRelationContext;
  class ColumnAliasesContext;
  class RelationPrimaryContext;
  class GraphColumnContext;
  class GraphPatternContext;
  class PathPatternContext;
  class PathPatternPrefixContext;
  class PathModeContext;
  class PathPatternExpressionContext;
  class PathFactorContext;
  class ParenthesizedPathPatternContext;
  class NodePatternContext;
  class EdgePatternContext;
  class ElementPatternFillerContext;
  class LabelExpressionContext;
  class LabelTermContext;
  class LabelFactorContext;
  class LabelPrimaryContext;
  class GraphQuantifierContext;
  class ExpressionContext;
  class BooleanExpressionContext;
  class PredicateContext;
  class ValueExpressionContext;
  class PrimaryExpressionContext;
  class StringContext;
  class NullTreatmentContext;
  class TimeZoneSpecifierContext;
  class ComparisonOperatorContext;
  class ComparisonQuantifierContext;
  class BooleanValueContext;
  class IntervalContext;
  class IntervalFieldContext;
  class NormalFormContext;
  class TypesContext;
  class TypeContext;
  class TypeParameterContext;
  class BaseTypeContext;
  class WhenClauseContext;
  class FilterContext;
  class OverContext;
  class WindowSpecificationContext;
  class WindowFrameContext;
  class FrameBoundContext;
  class UpdateAssignmentContext;
  class ExplainOptionContext;
  class TransactionModeContext;
  class LevelOfIsolationContext;
  class CallArgumentContext;
  class PrivilegeContext;
  class QualifiedNameContext;
  class TableVersionExpressionContext;
  class TableVersionStateContext;
  class GrantorContext;
  class PrincipalContext;
  class RolesContext;
  class IdentifierContext;
  class NumberContext;
  class ConstraintSpecificationContext;
  class NamedConstraintSpecificationContext;
  class UnnamedConstraintSpecificationContext;
  class ConstraintTypeContext;
  class ConstraintQualifiersContext;
  class ConstraintQualifierContext;
  class ConstraintRelyContext;
  class ConstraintEnabledContext;
  class ConstraintEnforcedContext;
  class NonReservedContext;

  class SingleStatementContext : public antlr4::ParserRuleContext {
   public:
    SingleStatementContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StatementContext* statement();
    antlr4::tree::TerminalNode* EOF();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SingleStatementContext* singleStatement();

  class StandaloneExpressionContext : public antlr4::ParserRuleContext {
   public:
    StandaloneExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* EOF();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StandaloneExpressionContext* standaloneExpression();

  class StandaloneRoutineBodyContext : public antlr4::ParserRuleContext {
   public:
    StandaloneRoutineBodyContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RoutineBodyContext* routineBody();
    antlr4::tree::TerminalNode* EOF();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StandaloneRoutineBodyContext* standaloneRoutineBody();

  class StatementContext : public antlr4::ParserRuleContext {
   public:
    StatementContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    StatementContext() = default;
    void copyFrom(StatementContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class ExplainContext : public StatementContext {
   public:
    ExplainContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* EXPLAIN();
    StatementContext* statement();
    antlr4::tree::TerminalNode* ANALYZE();
    antlr4::tree::TerminalNode* VERBOSE();
    std::vector<ExplainOptionContext*> explainOption();
    ExplainOptionContext* explainOption(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class PrepareContext : public StatementContext {
   public:
    PrepareContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* PREPARE();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* FROM();
    StatementContext* statement();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropMaterializedViewContext : public StatementContext {
   public:
    DropMaterializedViewContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* MATERIALIZED();
    antlr4::tree::TerminalNode* VIEW();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UseContext : public StatementContext {
   public:
    UseContext(StatementContext* ctx);

    PrestoSqlParser::IdentifierContext* schema = nullptr;
    PrestoSqlParser::IdentifierContext* catalog = nullptr;
    antlr4::tree::TerminalNode* USE();
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AddConstraintContext : public StatementContext {
   public:
    AddConstraintContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* tableName = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* ADD();
    ConstraintSpecificationContext* constraintSpecification();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DeallocateContext : public StatementContext {
   public:
    DeallocateContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DEALLOCATE();
    antlr4::tree::TerminalNode* PREPARE();
    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RenameTableContext : public StatementContext {
   public:
    RenameTableContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* from = nullptr;
    PrestoSqlParser::QualifiedNameContext* to = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* RENAME();
    antlr4::tree::TerminalNode* TO();
    std::vector<QualifiedNameContext*> qualifiedName();
    QualifiedNameContext* qualifiedName(size_t i);
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CommitContext : public StatementContext {
   public:
    CommitContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* COMMIT();
    antlr4::tree::TerminalNode* WORK();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateRoleContext : public StatementContext {
   public:
    CreateRoleContext(StatementContext* ctx);

    PrestoSqlParser::IdentifierContext* name = nullptr;
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* ROLE();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* WITH();
    antlr4::tree::TerminalNode* ADMIN();
    GrantorContext* grantor();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowCreateFunctionContext : public StatementContext {
   public:
    ShowCreateFunctionContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* FUNCTION();
    QualifiedNameContext* qualifiedName();
    TypesContext* types();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropColumnContext : public StatementContext {
   public:
    DropColumnContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* tableName = nullptr;
    PrestoSqlParser::QualifiedNameContext* column = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* COLUMN();
    std::vector<QualifiedNameContext*> qualifiedName();
    QualifiedNameContext* qualifiedName(size_t i);
    std::vector<antlr4::tree::TerminalNode*> IF();
    antlr4::tree::TerminalNode* IF(size_t i);
    std::vector<antlr4::tree::TerminalNode*> EXISTS();
    antlr4::tree::TerminalNode* EXISTS(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropViewContext : public StatementContext {
   public:
    DropViewContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* VIEW();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowTablesContext : public StatementContext {
   public:
    ShowTablesContext(StatementContext* ctx);

    PrestoSqlParser::StringContext* pattern = nullptr;
    PrestoSqlParser::StringContext* escape = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* TABLES();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* LIKE();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();
    std::vector<StringContext*> string();
    StringContext* string(size_t i);
    antlr4::tree::TerminalNode* ESCAPE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowCatalogsContext : public StatementContext {
   public:
    ShowCatalogsContext(StatementContext* ctx);

    PrestoSqlParser::StringContext* pattern = nullptr;
    PrestoSqlParser::StringContext* escape = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* CATALOGS();
    antlr4::tree::TerminalNode* LIKE();
    std::vector<StringContext*> string();
    StringContext* string(size_t i);
    antlr4::tree::TerminalNode* ESCAPE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowRolesContext : public StatementContext {
   public:
    ShowRolesContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* ROLES();
    antlr4::tree::TerminalNode* CURRENT();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RenameColumnContext : public StatementContext {
   public:
    RenameColumnContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* tableName = nullptr;
    PrestoSqlParser::IdentifierContext* from = nullptr;
    PrestoSqlParser::IdentifierContext* to = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* RENAME();
    antlr4::tree::TerminalNode* COLUMN();
    antlr4::tree::TerminalNode* TO();
    QualifiedNameContext* qualifiedName();
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    std::vector<antlr4::tree::TerminalNode*> IF();
    antlr4::tree::TerminalNode* IF(size_t i);
    std::vector<antlr4::tree::TerminalNode*> EXISTS();
    antlr4::tree::TerminalNode* EXISTS(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RevokeRolesContext : public StatementContext {
   public:
    RevokeRolesContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* REVOKE();
    RolesContext* roles();
    antlr4::tree::TerminalNode* FROM();
    std::vector<PrincipalContext*> principal();
    PrincipalContext* principal(size_t i);
    antlr4::tree::TerminalNode* ADMIN();
    antlr4::tree::TerminalNode* OPTION();
    antlr4::tree::TerminalNode* FOR();
    antlr4::tree::TerminalNode* GRANTED();
    antlr4::tree::TerminalNode* BY();
    GrantorContext* grantor();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowCreateTableContext : public StatementContext {
   public:
    ShowCreateTableContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* TABLE();
    QualifiedNameContext* qualifiedName();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowColumnsContext : public StatementContext {
   public:
    ShowColumnsContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* COLUMNS();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();
    antlr4::tree::TerminalNode* DESCRIBE();
    antlr4::tree::TerminalNode* DESC();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowRoleGrantsContext : public StatementContext {
   public:
    ShowRoleGrantsContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* ROLE();
    antlr4::tree::TerminalNode* GRANTS();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AddColumnContext : public StatementContext {
   public:
    AddColumnContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* tableName = nullptr;
    PrestoSqlParser::ColumnDefinitionContext* column = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* ADD();
    antlr4::tree::TerminalNode* COLUMN();
    QualifiedNameContext* qualifiedName();
    ColumnDefinitionContext* columnDefinition();
    std::vector<antlr4::tree::TerminalNode*> IF();
    antlr4::tree::TerminalNode* IF(size_t i);
    std::vector<antlr4::tree::TerminalNode*> EXISTS();
    antlr4::tree::TerminalNode* EXISTS(size_t i);
    antlr4::tree::TerminalNode* NOT();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ResetSessionContext : public StatementContext {
   public:
    ResetSessionContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* RESET();
    antlr4::tree::TerminalNode* SESSION();
    QualifiedNameContext* qualifiedName();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropConstraintContext : public StatementContext {
   public:
    DropConstraintContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* tableName = nullptr;
    PrestoSqlParser::IdentifierContext* name = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* CONSTRAINT();
    QualifiedNameContext* qualifiedName();
    IdentifierContext* identifier();
    std::vector<antlr4::tree::TerminalNode*> IF();
    antlr4::tree::TerminalNode* IF(size_t i);
    std::vector<antlr4::tree::TerminalNode*> EXISTS();
    antlr4::tree::TerminalNode* EXISTS(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class InsertIntoContext : public StatementContext {
   public:
    InsertIntoContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* INSERT();
    antlr4::tree::TerminalNode* INTO();
    QualifiedNameContext* qualifiedName();
    QueryContext* query();
    ColumnAliasesContext* columnAliases();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowSessionContext : public StatementContext {
   public:
    ShowSessionContext(StatementContext* ctx);

    PrestoSqlParser::StringContext* pattern = nullptr;
    PrestoSqlParser::StringContext* escape = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* SESSION();
    antlr4::tree::TerminalNode* LIKE();
    std::vector<StringContext*> string();
    StringContext* string(size_t i);
    antlr4::tree::TerminalNode* ESCAPE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateSchemaContext : public StatementContext {
   public:
    CreateSchemaContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* SCHEMA();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* WITH();
    PropertiesContext* properties();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ExecuteContext : public StatementContext {
   public:
    ExecuteContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* EXECUTE();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* USING();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RenameSchemaContext : public StatementContext {
   public:
    RenameSchemaContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* SCHEMA();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* RENAME();
    antlr4::tree::TerminalNode* TO();
    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropRoleContext : public StatementContext {
   public:
    DropRoleContext(StatementContext* ctx);

    PrestoSqlParser::IdentifierContext* name = nullptr;
    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* ROLE();
    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AnalyzeContext : public StatementContext {
   public:
    AnalyzeContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ANALYZE();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* WITH();
    PropertiesContext* properties();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetRoleContext : public StatementContext {
   public:
    SetRoleContext(StatementContext* ctx);

    PrestoSqlParser::IdentifierContext* role = nullptr;
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* ROLE();
    antlr4::tree::TerminalNode* ALL();
    antlr4::tree::TerminalNode* NONE();
    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateFunctionContext : public StatementContext {
   public:
    CreateFunctionContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* functionName = nullptr;
    PrestoSqlParser::TypeContext* returnType = nullptr;
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* FUNCTION();
    antlr4::tree::TerminalNode* RETURNS();
    RoutineCharacteristicsContext* routineCharacteristics();
    RoutineBodyContext* routineBody();
    QualifiedNameContext* qualifiedName();
    TypeContext* type();
    antlr4::tree::TerminalNode* OR();
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* TEMPORARY();
    std::vector<SqlParameterDeclarationContext*> sqlParameterDeclaration();
    SqlParameterDeclarationContext* sqlParameterDeclaration(size_t i);
    antlr4::tree::TerminalNode* COMMENT();
    StringContext* string();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowGrantsContext : public StatementContext {
   public:
    ShowGrantsContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* GRANTS();
    antlr4::tree::TerminalNode* ON();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* TABLE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropSchemaContext : public StatementContext {
   public:
    DropSchemaContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* SCHEMA();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* CASCADE();
    antlr4::tree::TerminalNode* RESTRICT();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowCreateViewContext : public StatementContext {
   public:
    ShowCreateViewContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* VIEW();
    QualifiedNameContext* qualifiedName();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateTableContext : public StatementContext {
   public:
    CreateTableContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* TABLE();
    QualifiedNameContext* qualifiedName();
    std::vector<TableElementContext*> tableElement();
    TableElementContext* tableElement(size_t i);
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* COMMENT();
    StringContext* string();
    antlr4::tree::TerminalNode* WITH();
    PropertiesContext* properties();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class StartTransactionContext : public StatementContext {
   public:
    StartTransactionContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* START();
    antlr4::tree::TerminalNode* TRANSACTION();
    std::vector<TransactionModeContext*> transactionMode();
    TransactionModeContext* transactionMode(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateTableAsSelectContext : public StatementContext {
   public:
    CreateTableAsSelectContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* TABLE();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* AS();
    QueryContext* query();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    ColumnAliasesContext* columnAliases();
    antlr4::tree::TerminalNode* COMMENT();
    StringContext* string();
    std::vector<antlr4::tree::TerminalNode*> WITH();
    antlr4::tree::TerminalNode* WITH(size_t i);
    PropertiesContext* properties();
    antlr4::tree::TerminalNode* DATA();
    antlr4::tree::TerminalNode* NO();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowStatsContext : public StatementContext {
   public:
    ShowStatsContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* STATS();
    antlr4::tree::TerminalNode* FOR();
    QualifiedNameContext* qualifiedName();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropFunctionContext : public StatementContext {
   public:
    DropFunctionContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* FUNCTION();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* TEMPORARY();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    TypesContext* types();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RevokeContext : public StatementContext {
   public:
    RevokeContext(StatementContext* ctx);

    PrestoSqlParser::PrincipalContext* grantee = nullptr;
    antlr4::tree::TerminalNode* REVOKE();
    antlr4::tree::TerminalNode* ON();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* FROM();
    PrincipalContext* principal();
    std::vector<PrivilegeContext*> privilege();
    PrivilegeContext* privilege(size_t i);
    antlr4::tree::TerminalNode* ALL();
    antlr4::tree::TerminalNode* PRIVILEGES();
    antlr4::tree::TerminalNode* GRANT();
    antlr4::tree::TerminalNode* OPTION();
    antlr4::tree::TerminalNode* FOR();
    antlr4::tree::TerminalNode* TABLE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UpdateContext : public StatementContext {
   public:
    UpdateContext(StatementContext* ctx);

    PrestoSqlParser::BooleanExpressionContext* where = nullptr;
    antlr4::tree::TerminalNode* UPDATE();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* SET();
    std::vector<UpdateAssignmentContext*> updateAssignment();
    UpdateAssignmentContext* updateAssignment(size_t i);
    antlr4::tree::TerminalNode* WHERE();
    BooleanExpressionContext* booleanExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateTypeContext : public StatementContext {
   public:
    CreateTypeContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* TYPE();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* AS();
    std::vector<SqlParameterDeclarationContext*> sqlParameterDeclaration();
    SqlParameterDeclarationContext* sqlParameterDeclaration(size_t i);
    TypeContext* type();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DeleteContext : public StatementContext {
   public:
    DeleteContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DELETE();
    antlr4::tree::TerminalNode* FROM();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* WHERE();
    BooleanExpressionContext* booleanExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DescribeInputContext : public StatementContext {
   public:
    DescribeInputContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DESCRIBE();
    antlr4::tree::TerminalNode* INPUT();
    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowStatsForQueryContext : public StatementContext {
   public:
    ShowStatsForQueryContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* STATS();
    antlr4::tree::TerminalNode* FOR();
    QuerySpecificationContext* querySpecification();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class StatementDefaultContext : public StatementContext {
   public:
    StatementDefaultContext(StatementContext* ctx);

    QueryContext* query();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TruncateTableContext : public StatementContext {
   public:
    TruncateTableContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* TRUNCATE();
    antlr4::tree::TerminalNode* TABLE();
    QualifiedNameContext* qualifiedName();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AlterColumnSetNotNullContext : public StatementContext {
   public:
    AlterColumnSetNotNullContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* tableName = nullptr;
    PrestoSqlParser::IdentifierContext* column = nullptr;
    std::vector<antlr4::tree::TerminalNode*> ALTER();
    antlr4::tree::TerminalNode* ALTER(size_t i);
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    QualifiedNameContext* qualifiedName();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* COLUMN();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateMaterializedViewContext : public StatementContext {
   public:
    CreateMaterializedViewContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* MATERIALIZED();
    antlr4::tree::TerminalNode* VIEW();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* AS();
    QueryContext* query();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* COMMENT();
    StringContext* string();
    antlr4::tree::TerminalNode* WITH();
    PropertiesContext* properties();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AlterFunctionContext : public StatementContext {
   public:
    AlterFunctionContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* FUNCTION();
    QualifiedNameContext* qualifiedName();
    AlterRoutineCharacteristicsContext* alterRoutineCharacteristics();
    TypesContext* types();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetSessionContext : public StatementContext {
   public:
    SetSessionContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* SESSION();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* EQ();
    ExpressionContext* expression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateViewContext : public StatementContext {
   public:
    CreateViewContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* VIEW();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* AS();
    QueryContext* query();
    antlr4::tree::TerminalNode* OR();
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* SECURITY();
    antlr4::tree::TerminalNode* DEFINER();
    antlr4::tree::TerminalNode* INVOKER();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowSchemasContext : public StatementContext {
   public:
    ShowSchemasContext(StatementContext* ctx);

    PrestoSqlParser::StringContext* pattern = nullptr;
    PrestoSqlParser::StringContext* escape = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* SCHEMAS();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* LIKE();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();
    std::vector<StringContext*> string();
    StringContext* string(size_t i);
    antlr4::tree::TerminalNode* ESCAPE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropTableContext : public StatementContext {
   public:
    DropTableContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* TABLE();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RollbackContext : public StatementContext {
   public:
    RollbackContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ROLLBACK();
    antlr4::tree::TerminalNode* WORK();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RenameViewContext : public StatementContext {
   public:
    RenameViewContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* from = nullptr;
    PrestoSqlParser::QualifiedNameContext* to = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* VIEW();
    antlr4::tree::TerminalNode* RENAME();
    antlr4::tree::TerminalNode* TO();
    std::vector<QualifiedNameContext*> qualifiedName();
    QualifiedNameContext* qualifiedName(size_t i);
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AlterColumnDropNotNullContext : public StatementContext {
   public:
    AlterColumnDropNotNullContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* tableName = nullptr;
    PrestoSqlParser::IdentifierContext* column = nullptr;
    std::vector<antlr4::tree::TerminalNode*> ALTER();
    antlr4::tree::TerminalNode* ALTER(size_t i);
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    QualifiedNameContext* qualifiedName();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* COLUMN();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class GrantRolesContext : public StatementContext {
   public:
    GrantRolesContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* GRANT();
    RolesContext* roles();
    antlr4::tree::TerminalNode* TO();
    std::vector<PrincipalContext*> principal();
    PrincipalContext* principal(size_t i);
    antlr4::tree::TerminalNode* WITH();
    antlr4::tree::TerminalNode* ADMIN();
    antlr4::tree::TerminalNode* OPTION();
    antlr4::tree::TerminalNode* GRANTED();
    antlr4::tree::TerminalNode* BY();
    GrantorContext* grantor();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CallContext : public StatementContext {
   public:
    CallContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CALL();
    QualifiedNameContext* qualifiedName();
    std::vector<CallArgumentContext*> callArgument();
    CallArgumentContext* callArgument(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RefreshMaterializedViewContext : public StatementContext {
   public:
    RefreshMaterializedViewContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* REFRESH();
    antlr4::tree::TerminalNode* MATERIALIZED();
    antlr4::tree::TerminalNode* VIEW();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* WHERE();
    BooleanExpressionContext* booleanExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowCreateMaterializedViewContext : public StatementContext {
   public:
    ShowCreateMaterializedViewContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* MATERIALIZED();
    antlr4::tree::TerminalNode* VIEW();
    QualifiedNameContext* qualifiedName();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowFunctionsContext : public StatementContext {
   public:
    ShowFunctionsContext(StatementContext* ctx);

    PrestoSqlParser::StringContext* pattern = nullptr;
    PrestoSqlParser::StringContext* escape = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* FUNCTIONS();
    antlr4::tree::TerminalNode* LIKE();
    std::vector<StringContext*> string();
    StringContext* string(size_t i);
    antlr4::tree::TerminalNode* ESCAPE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DescribeOutputContext : public StatementContext {
   public:
    DescribeOutputContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DESCRIBE();
    antlr4::tree::TerminalNode* OUTPUT();
    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class GrantContext : public StatementContext {
   public:
    GrantContext(StatementContext* ctx);

    PrestoSqlParser::PrincipalContext* grantee = nullptr;
    std::vector<antlr4::tree::TerminalNode*> GRANT();
    antlr4::tree::TerminalNode* GRANT(size_t i);
    antlr4::tree::TerminalNode* ON();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* TO();
    PrincipalContext* principal();
    std::vector<PrivilegeContext*> privilege();
    PrivilegeContext* privilege(size_t i);
    antlr4::tree::TerminalNode* ALL();
    antlr4::tree::TerminalNode* PRIVILEGES();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* WITH();
    antlr4::tree::TerminalNode* OPTION();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetTablePropertiesContext : public StatementContext {
   public:
    SetTablePropertiesContext(StatementContext* ctx);

    PrestoSqlParser::QualifiedNameContext* tableName = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* PROPERTIES();
    PropertiesContext* properties();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StatementContext* statement();

  class QueryContext : public antlr4::ParserRuleContext {
   public:
    QueryContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryNoWithContext* queryNoWith();
    WithContext* with();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QueryContext* query();

  class WithContext : public antlr4::ParserRuleContext {
   public:
    WithContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WITH();
    std::vector<WithElementContext*> withElement();
    WithElementContext* withElement(size_t i);
    antlr4::tree::TerminalNode* RECURSIVE();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WithContext* with();

  class WithElementContext : public antlr4::ParserRuleContext {
   public:
    WithElementContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NamedQueryContext* namedQuery();
    NamedGraphContext* namedGraph();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WithElementContext* withElement();

  class TableElementContext : public antlr4::ParserRuleContext {
   public:
    TableElementContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ConstraintSpecificationContext* constraintSpecification();
    ColumnDefinitionContext* columnDefinition();
    LikeClauseContext* likeClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TableElementContext* tableElement();

  class ColumnDefinitionContext : public antlr4::ParserRuleContext {
   public:
    ColumnDefinitionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    TypeContext* type();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    antlr4::tree::TerminalNode* COMMENT();
    StringContext* string();
    antlr4::tree::TerminalNode* WITH();
    PropertiesContext* properties();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ColumnDefinitionContext* columnDefinition();

  class LikeClauseContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* optionType = nullptr;
    LikeClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LIKE();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* PROPERTIES();
    antlr4::tree::TerminalNode* INCLUDING();
    antlr4::tree::TerminalNode* EXCLUDING();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LikeClauseContext* likeClause();

  class PropertiesContext : public antlr4::ParserRuleContext {
   public:
    PropertiesContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PropertyContext*> property();
    PropertyContext* property(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertiesContext* properties();

  class PropertyContext : public antlr4::ParserRuleContext {
   public:
    PropertyContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* EQ();
    ExpressionContext* expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyContext* property();

  class SqlParameterDeclarationContext : public antlr4::ParserRuleContext {
   public:
    SqlParameterDeclarationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    TypeContext* type();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SqlParameterDeclarationContext* sqlParameterDeclaration();

  class RoutineCharacteristicsContext : public antlr4::ParserRuleContext {
   public:
    RoutineCharacteristicsContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<RoutineCharacteristicContext*> routineCharacteristic();
    RoutineCharacteristicContext* routineCharacteristic(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RoutineCharacteristicsContext* routineCharacteristics();

  class RoutineCharacteristicContext : public antlr4::ParserRuleContext {
   public:
    RoutineCharacteristicContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LANGUAGE();
    LanguageContext* language();
    DeterminismContext* determinism();
    NullCallClauseContext* nullCallClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RoutineCharacteristicContext* routineCharacteristic();

  class AlterRoutineCharacteristicsContext : public antlr4::ParserRuleContext {
   public:
    AlterRoutineCharacteristicsContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AlterRoutineCharacteristicContext*>
    alterRoutineCharacteristic();
    AlterRoutineCharacteristicContext* alterRoutineCharacteristic(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AlterRoutineCharacteristicsContext* alterRoutineCharacteristics();

  class AlterRoutineCharacteristicContext : public antlr4::ParserRuleContext {
   public:
    AlterRoutineCharacteristicContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NullCallClauseContext* nullCallClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AlterRoutineCharacteristicContext* alterRoutineCharacteristic();

  class RoutineBodyContext : public antlr4::ParserRuleContext {
   public:
    RoutineBodyContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ReturnStatementContext* returnStatement();
    ExternalBodyReferenceContext* externalBodyReference();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RoutineBodyContext* routineBody();

  class ReturnStatementContext : public antlr4::ParserRuleContext {
   public:
    ReturnStatementContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* RETURN();
    ExpressionContext* expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ReturnStatementContext* returnStatement();

  class ExternalBodyReferenceContext : public antlr4::ParserRuleContext {
   public:
    ExternalBodyReferenceContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* EXTERNAL();
    antlr4::tree::TerminalNode* NAME();
    ExternalRoutineNameContext* externalRoutineName();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ExternalBodyReferenceContext* externalBodyReference();

  class LanguageContext : public antlr4::ParserRuleContext {
   public:
    LanguageContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SQL();
    IdentifierContext* identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LanguageContext* language();

  class DeterminismContext : public antlr4::ParserRuleContext {
   public:
    DeterminismContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* DETERMINISTIC();
    antlr4::tree::TerminalNode* NOT();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DeterminismContext* determinism();

  class NullCallClauseContext : public antlr4::ParserRuleContext {
   public:
    NullCallClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* RETURNS();
    std::vector<antlr4::tree::TerminalNode*> NULL_LITERAL();
    antlr4::tree::TerminalNode* NULL_LITERAL(size_t i);
    antlr4::tree::TerminalNode* ON();
    antlr4::tree::TerminalNode* INPUT();
    antlr4::tree::TerminalNode* CALLED();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NullCallClauseContext* nullCallClause();

  class ExternalRoutineNameContext : public antlr4::ParserRuleContext {
   public:
    ExternalRoutineNameContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ExternalRoutineNameContext* externalRoutineName();

  class QueryNoWithContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* offset = nullptr;
    antlr4::Token* limit = nullptr;
    antlr4::Token* fetchFirstNRows = nullptr;
    QueryNoWithContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryTermContext* queryTerm();
    antlr4::tree::TerminalNode* ORDER();
    antlr4::tree::TerminalNode* BY();
    std::vector<SortItemContext*> sortItem();
    SortItemContext* sortItem(size_t i);
    antlr4::tree::TerminalNode* OFFSET();
    std::vector<antlr4::tree::TerminalNode*> INTEGER_VALUE();
    antlr4::tree::TerminalNode* INTEGER_VALUE(size_t i);
    antlr4::tree::TerminalNode* LIMIT();
    antlr4::tree::TerminalNode* ROW();
    std::vector<antlr4::tree::TerminalNode*> ROWS();
    antlr4::tree::TerminalNode* ROWS(size_t i);
    antlr4::tree::TerminalNode* ALL();
    antlr4::tree::TerminalNode* FETCH();
    antlr4::tree::TerminalNode* FIRST();
    antlr4::tree::TerminalNode* ONLY();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QueryNoWithContext* queryNoWith();

  class QueryTermContext : public antlr4::ParserRuleContext {
   public:
    QueryTermContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    QueryTermContext() = default;
    void copyFrom(QueryTermContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class QueryTermDefaultContext : public QueryTermContext {
   public:
    QueryTermDefaultContext(QueryTermContext* ctx);

    QueryPrimaryContext* queryPrimary();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetOperationContext : public QueryTermContext {
   public:
    SetOperationContext(QueryTermContext* ctx);

    PrestoSqlParser::QueryTermContext* left = nullptr;
    antlr4::Token* op = nullptr;
    PrestoSqlParser::QueryTermContext* right = nullptr;
    std::vector<QueryTermContext*> queryTerm();
    QueryTermContext* queryTerm(size_t i);
    antlr4::tree::TerminalNode* INTERSECT();
    SetQuantifierContext* setQuantifier();
    antlr4::tree::TerminalNode* UNION();
    antlr4::tree::TerminalNode* EXCEPT();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QueryTermContext* queryTerm();
  QueryTermContext* queryTerm(int precedence);
  class QueryPrimaryContext : public antlr4::ParserRuleContext {
   public:
    QueryPrimaryContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    QueryPrimaryContext() = default;
    void copyFrom(QueryPrimaryContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class SubqueryContext : public QueryPrimaryContext {
   public:
    SubqueryContext(QueryPrimaryContext* ctx);

    QueryNoWithContext* queryNoWith();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class QueryPrimaryDefaultContext : public QueryPrimaryContext {
   public:
    QueryPrimaryDefaultContext(QueryPrimaryContext* ctx);

    QuerySpecificationContext* querySpecification();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TableContext : public QueryPrimaryContext {
   public:
    TableContext(QueryPrimaryContext* ctx);

    antlr4::tree::TerminalNode* TABLE();
    QualifiedNameContext* qualifiedName();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class InlineTableContext : public QueryPrimaryContext {
   public:
    InlineTableContext(QueryPrimaryContext* ctx);

    antlr4::tree::TerminalNode* VALUES();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QueryPrimaryContext* queryPrimary();

  class SortItemContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* ordering = nullptr;
    antlr4::Token* nullOrdering = nullptr;
    SortItemContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* NULLS();
    antlr4::tree::TerminalNode* ASC();
    antlr4::tree::TerminalNode* DESC();
    antlr4::tree::TerminalNode* FIRST();
    antlr4::tree::TerminalNode* LAST();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SortItemContext* sortItem();

  class QuerySpecificationContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::BooleanExpressionContext* where = nullptr;
    PrestoSqlParser::BooleanExpressionContext* having = nullptr;
    QuerySpecificationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SELECT();
    std::vector<SelectItemContext*> selectItem();
    SelectItemContext* selectItem(size_t i);
    SetQuantifierContext* setQuantifier();
    antlr4::tree::TerminalNode* FROM();
    std::vector<RelationContext*> relation();
    RelationContext* relation(size_t i);
    antlr4::tree::TerminalNode* WHERE();
    antlr4::tree::TerminalNode* GROUP();
    antlr4::tree::TerminalNode* BY();
    GroupByContext* groupBy();
    antlr4::tree::TerminalNode* HAVING();
    antlr4::tree::TerminalNode* WINDOW();
    std::vector<WindowDefinitionContext*> windowDefinition();
    WindowDefinitionContext* windowDefinition(size_t i);
    std::vector<BooleanExpressionContext*> booleanExpression();
    BooleanExpressionContext* booleanExpression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QuerySpecificationContext* querySpecification();

  class WindowDefinitionContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* name = nullptr;
    WindowDefinitionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* AS();
    WindowSpecificationContext* windowSpecification();
    IdentifierContext* identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WindowDefinitionContext* windowDefinition();

  class GroupByContext : public antlr4::ParserRuleContext {
   public:
    GroupByContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<GroupingElementContext*> groupingElement();
    GroupingElementContext* groupingElement(size_t i);
    SetQuantifierContext* setQuantifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupByContext* groupBy();

  class GroupingElementContext : public antlr4::ParserRuleContext {
   public:
    GroupingElementContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    GroupingElementContext() = default;
    void copyFrom(GroupingElementContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class MultipleGroupingSetsContext : public GroupingElementContext {
   public:
    MultipleGroupingSetsContext(GroupingElementContext* ctx);

    antlr4::tree::TerminalNode* GROUPING();
    antlr4::tree::TerminalNode* SETS();
    std::vector<GroupingSetContext*> groupingSet();
    GroupingSetContext* groupingSet(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SingleGroupingSetContext : public GroupingElementContext {
   public:
    SingleGroupingSetContext(GroupingElementContext* ctx);

    GroupingSetContext* groupingSet();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CubeContext : public GroupingElementContext {
   public:
    CubeContext(GroupingElementContext* ctx);

    antlr4::tree::TerminalNode* CUBE();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RollupContext : public GroupingElementContext {
   public:
    RollupContext(GroupingElementContext* ctx);

    antlr4::tree::TerminalNode* ROLLUP();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupingElementContext* groupingElement();

  class GroupingSetContext : public antlr4::ParserRuleContext {
   public:
    GroupingSetContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupingSetContext* groupingSet();

  class NamedQueryContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* name = nullptr;
    NamedQueryContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* AS();
    QueryContext* query();
    IdentifierContext* identifier();
    ColumnAliasesContext* columnAliases();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NamedQueryContext* namedQuery();

  class NamedGraphContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* name = nullptr;
    NamedGraphContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* GRAPH();
    antlr4::tree::TerminalNode* AS();
    antlr4::tree::TerminalNode* VERTEX();
    std::vector<antlr4::tree::TerminalNode*> TABLES();
    antlr4::tree::TerminalNode* TABLES(size_t i);
    std::vector<VertexTableContext*> vertexTable();
    VertexTableContext* vertexTable(size_t i);
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* EDGE();
    std::vector<EdgeTableContext*> edgeTable();
    EdgeTableContext* edgeTable(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NamedGraphContext* namedGraph();

  class VertexTableContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* alias = nullptr;
    VertexTableContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedNameContext* qualifiedName();
    GraphElementKeyContext* graphElementKey();
    antlr4::tree::TerminalNode* AS();
    GraphElementLabelsContext* graphElementLabels();
    IdentifierContext* identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VertexTableContext* vertexTable();

  class EdgeTableContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* alias = nullptr;
    PrestoSqlParser::EdgeEndpointContext* source = nullptr;
    PrestoSqlParser::EdgeEndpointContext* destination = nullptr;
    EdgeTableContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedNameContext* qualifiedName();
    GraphElementKeyContext* graphElementKey();
    antlr4::tree::TerminalNode* SOURCE();
    antlr4::tree::TerminalNode* DESTINATION();
    std::vector<EdgeEndpointContext*> edgeEndpoint();
    EdgeEndpointContext* edgeEndpoint(size_t i);
    antlr4::tree::TerminalNode* AS();
    GraphElementLabelsContext* graphElementLabels();
    IdentifierContext* identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  EdgeTableContext* edgeTable();

  class GraphElementKeyContext : public antlr4::ParserRuleContext {
   public:
    GraphElementKeyContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* KEY();
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphElementKeyContext* graphElementKey();

  class EdgeEndpointContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* vertexTableName = nullptr;
    PrestoSqlParser::IdentifierContext* identifierContext = nullptr;
    std::vector<IdentifierContext*> referencedColumns;
    EdgeEndpointContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GraphElementKeyContext* graphElementKey();
    antlr4::tree::TerminalNode* REFERENCES();
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  EdgeEndpointContext* edgeEndpoint();

  class GraphElementLabelsContext : public antlr4::ParserRuleContext {
   public:
    GraphElementLabelsContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    GraphElementLabelsContext() = default;
    void copyFrom(GraphElementLabelsContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class DefaultGraphLabelContext : public GraphElementLabelsContext {
   public:
    DefaultGraphLabelContext(GraphElementLabelsContext* ctx);

    GraphElementPropertiesContext* graphElementProperties();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ExplicitGraphLabelsContext : public GraphElementLabelsContext {
   public:
    ExplicitGraphLabelsContext(GraphElementLabelsContext* ctx);

    std::vector<antlr4::tree::TerminalNode*> LABEL();
    antlr4::tree::TerminalNode* LABEL(size_t i);
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    std::vector<GraphElementPropertiesContext*> graphElementProperties();
    GraphElementPropertiesContext* graphElementProperties(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphElementLabelsContext* graphElementLabels();

  class GraphElementPropertiesContext : public antlr4::ParserRuleContext {
   public:
    GraphElementPropertiesContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* PROPERTIES();
    std::vector<GraphElementPropertyContext*> graphElementProperty();
    GraphElementPropertyContext* graphElementProperty(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphElementPropertiesContext* graphElementProperties();

  class GraphElementPropertyContext : public antlr4::ParserRuleContext {
   public:
    GraphElementPropertyContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* AS();
    IdentifierContext* identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphElementPropertyContext* graphElementProperty();

  class SetQuantifierContext : public antlr4::ParserRuleContext {
   public:
    SetQuantifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* DISTINCT();
    antlr4::tree::TerminalNode* ALL();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SetQuantifierContext* setQuantifier();

  class SelectItemContext : public antlr4::ParserRuleContext {
   public:
    SelectItemContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    SelectItemContext() = default;
    void copyFrom(SelectItemContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class SelectAllContext : public SelectItemContext {
   public:
    SelectAllContext(SelectItemContext* ctx);

    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* ASTERISK();
    StarModifiersContext* starModifiers();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SelectSingleContext : public SelectItemContext {
   public:
    SelectSingleContext(SelectItemContext* ctx);

    ExpressionContext* expression();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* AS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SelectColumnsContext : public SelectItemContext {
   public:
    SelectColumnsContext(SelectItemContext* ctx);

    antlr4::Token* pattern = nullptr;
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* COLUMNS();
    antlr4::tree::TerminalNode* STRING();
    StarModifiersContext* starModifiers();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SelectItemContext* selectItem();

  class StarModifiersContext : public antlr4::ParserRuleContext {
   public:
    StarModifiersContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ExcludeClauseContext*> excludeClause();
    ExcludeClauseContext* excludeClause(size_t i);
    std::vector<ReplaceClauseContext*> replaceClause();
    ReplaceClauseContext* replaceClause(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StarModifiersContext* starModifiers();

  class ExcludeClauseContext : public antlr4::ParserRuleContext {
   public:
    ExcludeClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* EXCLUDE();
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ExcludeClauseContext* excludeClause();

  class ReplaceClauseContext : public antlr4::ParserRuleContext {
   public:
    ReplaceClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* REPLACE();
    std::vector<ReplaceItemContext*> replaceItem();
    ReplaceItemContext* replaceItem(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ReplaceClauseContext* replaceClause();

  class ReplaceItemContext : public antlr4::ParserRuleContext {
   public:
    ReplaceItemContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* AS();
    IdentifierContext* identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ReplaceItemContext* replaceItem();

  class RelationContext : public antlr4::ParserRuleContext {
   public:
    RelationContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    RelationContext() = default;
    void copyFrom(RelationContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class RelationDefaultContext : public RelationContext {
   public:
    RelationDefaultContext(RelationContext* ctx);

    SampledRelationContext* sampledRelation();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class JoinRelationContext : public RelationContext {
   public:
    JoinRelationContext(RelationContext* ctx);

    PrestoSqlParser::RelationContext* left = nullptr;
    PrestoSqlParser::SampledRelationContext* right = nullptr;
    PrestoSqlParser::RelationContext* rightRelation = nullptr;
    std::vector<RelationContext*> relation();
    RelationContext* relation(size_t i);
    antlr4::tree::TerminalNode* CROSS();
    antlr4::tree::TerminalNode* JOIN();
    JoinTypeContext* joinType();
    JoinCriteriaContext* joinCriteria();
    antlr4::tree::TerminalNode* NATURAL();
    SampledRelationContext* sampledRelation();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RelationContext* relation();
  RelationContext* relation(int precedence);
  class JoinTypeContext : public antlr4::ParserRuleContext {
   public:
    JoinTypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INNER();
    antlr4::tree::TerminalNode* LEFT();
    antlr4::tree::TerminalNode* OUTER();
    antlr4::tree::TerminalNode* RIGHT();
    antlr4::tree::TerminalNode* FULL();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  JoinTypeContext* joinType();

  class JoinCriteriaContext : public antlr4::ParserRuleContext {
   public:
    JoinCriteriaContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ON();
    BooleanExpressionContext* booleanExpression();
    antlr4::tree::TerminalNode* USING();
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  JoinCriteriaContext* joinCriteria();

  class SampledRelationContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::ExpressionContext* percentage = nullptr;
    SampledRelationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AliasedRelationContext* aliasedRelation();
    antlr4::tree::TerminalNode* TABLESAMPLE();
    SampleTypeContext* sampleType();
    ExpressionContext* expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SampledRelationContext* sampledRelation();

  class SampleTypeContext : public antlr4::ParserRuleContext {
   public:
    SampleTypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* BERNOULLI();
    antlr4::tree::TerminalNode* SYSTEM();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SampleTypeContext* sampleType();

  class AliasedRelationContext : public antlr4::ParserRuleContext {
   public:
    AliasedRelationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RelationPrimaryContext* relationPrimary();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* AS();
    ColumnAliasesContext* columnAliases();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AliasedRelationContext* aliasedRelation();

  class ColumnAliasesContext : public antlr4::ParserRuleContext {
   public:
    ColumnAliasesContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ColumnAliasesContext* columnAliases();

  class RelationPrimaryContext : public antlr4::ParserRuleContext {
   public:
    RelationPrimaryContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    RelationPrimaryContext() = default;
    void copyFrom(RelationPrimaryContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class SubqueryRelationContext : public RelationPrimaryContext {
   public:
    SubqueryRelationContext(RelationPrimaryContext* ctx);

    QueryContext* query();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ParenthesizedRelationContext : public RelationPrimaryContext {
   public:
    ParenthesizedRelationContext(RelationPrimaryContext* ctx);

    RelationContext* relation();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class GraphTableContext : public RelationPrimaryContext {
   public:
    GraphTableContext(RelationPrimaryContext* ctx);

    antlr4::tree::TerminalNode* GRAPH_TABLE();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* MATCH();
    GraphPatternContext* graphPattern();
    antlr4::tree::TerminalNode* COLUMNS();
    std::vector<GraphColumnContext*> graphColumn();
    GraphColumnContext* graphColumn(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UnnestContext : public RelationPrimaryContext {
   public:
    UnnestContext(RelationPrimaryContext* ctx);

    antlr4::tree::TerminalNode* UNNEST();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* WITH();
    antlr4::tree::TerminalNode* ORDINALITY();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LateralContext : public RelationPrimaryContext {
   public:
    LateralContext(RelationPrimaryContext* ctx);

    antlr4::tree::TerminalNode* LATERAL();
    QueryContext* query();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TableNameContext : public RelationPrimaryContext {
   public:
    TableNameContext(RelationPrimaryContext* ctx);

    QualifiedNameContext* qualifiedName();
    TableVersionExpressionContext* tableVersionExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RelationPrimaryContext* relationPrimary();

  class GraphColumnContext : public antlr4::ParserRuleContext {
   public:
    GraphColumnContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* AS();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphColumnContext* graphColumn();

  class GraphPatternContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::BooleanExpressionContext* where = nullptr;
    GraphPatternContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PathPatternContext*> pathPattern();
    PathPatternContext* pathPattern(size_t i);
    antlr4::tree::TerminalNode* WHERE();
    BooleanExpressionContext* booleanExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphPatternContext* graphPattern();

  class PathPatternContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* variable = nullptr;
    PathPatternContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PathPatternExpressionContext* pathPatternExpression();
    antlr4::tree::TerminalNode* EQ();
    PathPatternPrefixContext* pathPatternPrefix();
    IdentifierContext* identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathPatternContext* pathPattern();

  class PathPatternPrefixContext : public antlr4::ParserRuleContext {
   public:
    PathPatternPrefixContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    PathPatternPrefixContext() = default;
    void copyFrom(PathPatternPrefixContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class PathModePrefixContext : public PathPatternPrefixContext {
   public:
    PathModePrefixContext(PathPatternPrefixContext* ctx);

    PathModeContext* pathMode();
    antlr4::tree::TerminalNode* PATH();
    antlr4::tree::TerminalNode* PATHS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AllShortestPathsContext : public PathPatternPrefixContext {
   public:
    AllShortestPathsContext(PathPatternPrefixContext* ctx);

    antlr4::tree::TerminalNode* ALL();
    antlr4::tree::TerminalNode* SHORTEST();
    PathModeContext* pathMode();
    antlr4::tree::TerminalNode* PATH();
    antlr4::tree::TerminalNode* PATHS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CountedShortestPathsContext : public PathPatternPrefixContext {
   public:
    CountedShortestPathsContext(PathPatternPrefixContext* ctx);

    antlr4::Token* count = nullptr;
    antlr4::tree::TerminalNode* SHORTEST();
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    PathModeContext* pathMode();
    antlr4::tree::TerminalNode* PATH();
    antlr4::tree::TerminalNode* PATHS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CountedShortestGroupsContext : public PathPatternPrefixContext {
   public:
    CountedShortestGroupsContext(PathPatternPrefixContext* ctx);

    antlr4::Token* count = nullptr;
    antlr4::tree::TerminalNode* SHORTEST();
    antlr4::tree::TerminalNode* GROUP();
    antlr4::tree::TerminalNode* GROUPS();
    PathModeContext* pathMode();
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    antlr4::tree::TerminalNode* PATH();
    antlr4::tree::TerminalNode* PATHS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AnyPathsContext : public PathPatternPrefixContext {
   public:
    AnyPathsContext(PathPatternPrefixContext* ctx);

    antlr4::Token* count = nullptr;
    antlr4::tree::TerminalNode* ANY();
    PathModeContext* pathMode();
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    antlr4::tree::TerminalNode* PATH();
    antlr4::tree::TerminalNode* PATHS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AllPathsContext : public PathPatternPrefixContext {
   public:
    AllPathsContext(PathPatternPrefixContext* ctx);

    antlr4::tree::TerminalNode* ALL();
    PathModeContext* pathMode();
    antlr4::tree::TerminalNode* PATH();
    antlr4::tree::TerminalNode* PATHS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AnyShortestPathContext : public PathPatternPrefixContext {
   public:
    AnyShortestPathContext(PathPatternPrefixContext* ctx);

    antlr4::tree::TerminalNode* ANY();
    antlr4::tree::TerminalNode* SHORTEST();
    PathModeContext* pathMode();
    antlr4::tree::TerminalNode* PATH();
    antlr4::tree::TerminalNode* PATHS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathPatternPrefixContext* pathPatternPrefix();

  class PathModeContext : public antlr4::ParserRuleContext {
   public:
    PathModeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WALK();
    antlr4::tree::TerminalNode* TRAIL();
    antlr4::tree::TerminalNode* ACYCLIC();
    antlr4::tree::TerminalNode* SIMPLE();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathModeContext* pathMode();

  class PathPatternExpressionContext : public antlr4::ParserRuleContext {
   public:
    PathPatternExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PathFactorContext*> pathFactor();
    PathFactorContext* pathFactor(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathPatternExpressionContext* pathPatternExpression();

  class PathFactorContext : public antlr4::ParserRuleContext {
   public:
    PathFactorContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    PathFactorContext() = default;
    void copyFrom(PathFactorContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class NodePatternFactorContext : public PathFactorContext {
   public:
    NodePatternFactorContext(PathFactorContext* ctx);

    NodePatternContext* nodePattern();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ParenthesizedPathFactorContext : public PathFactorContext {
   public:
    ParenthesizedPathFactorContext(PathFactorContext* ctx);

    antlr4::Token* optional = nullptr;
    ParenthesizedPathPatternContext* parenthesizedPathPattern();
    GraphQuantifierContext* graphQuantifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class EdgePatternFactorContext : public PathFactorContext {
   public:
    EdgePatternFactorContext(PathFactorContext* ctx);

    EdgePatternContext* edgePattern();
    GraphQuantifierContext* graphQuantifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathFactorContext* pathFactor();

  class ParenthesizedPathPatternContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* variable = nullptr;
    PrestoSqlParser::BooleanExpressionContext* where = nullptr;
    ParenthesizedPathPatternContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PathPatternExpressionContext* pathPatternExpression();
    antlr4::tree::TerminalNode* EQ();
    PathModeContext* pathMode();
    antlr4::tree::TerminalNode* WHERE();
    IdentifierContext* identifier();
    BooleanExpressionContext* booleanExpression();
    antlr4::tree::TerminalNode* PATH();
    antlr4::tree::TerminalNode* PATHS();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ParenthesizedPathPatternContext* parenthesizedPathPattern();

  class NodePatternContext : public antlr4::ParserRuleContext {
   public:
    NodePatternContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ElementPatternFillerContext* elementPatternFiller();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NodePatternContext* nodePattern();

  class EdgePatternContext : public antlr4::ParserRuleContext {
   public:
    EdgePatternContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    EdgePatternContext() = default;
    void copyFrom(EdgePatternContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class LeftEdgeContext : public EdgePatternContext {
   public:
    LeftEdgeContext(EdgePatternContext* ctx);

    antlr4::tree::TerminalNode* LT();
    std::vector<antlr4::tree::TerminalNode*> MINUS();
    antlr4::tree::TerminalNode* MINUS(size_t i);
    ElementPatternFillerContext* elementPatternFiller();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AnyDirectionEdgeContext : public EdgePatternContext {
   public:
    AnyDirectionEdgeContext(EdgePatternContext* ctx);

    std::vector<antlr4::tree::TerminalNode*> MINUS();
    antlr4::tree::TerminalNode* MINUS(size_t i);
    ElementPatternFillerContext* elementPatternFiller();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RightEdgeContext : public EdgePatternContext {
   public:
    RightEdgeContext(EdgePatternContext* ctx);

    antlr4::tree::TerminalNode* MINUS();
    ElementPatternFillerContext* elementPatternFiller();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  EdgePatternContext* edgePattern();

  class ElementPatternFillerContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* variable = nullptr;
    PrestoSqlParser::BooleanExpressionContext* where = nullptr;
    ElementPatternFillerContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* IS();
    LabelExpressionContext* labelExpression();
    antlr4::tree::TerminalNode* WHERE();
    IdentifierContext* identifier();
    BooleanExpressionContext* booleanExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ElementPatternFillerContext* elementPatternFiller();

  class LabelExpressionContext : public antlr4::ParserRuleContext {
   public:
    LabelExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<LabelTermContext*> labelTerm();
    LabelTermContext* labelTerm(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LabelExpressionContext* labelExpression();

  class LabelTermContext : public antlr4::ParserRuleContext {
   public:
    LabelTermContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<LabelFactorContext*> labelFactor();
    LabelFactorContext* labelFactor(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LabelTermContext* labelTerm();

  class LabelFactorContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* negation = nullptr;
    LabelFactorContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LabelPrimaryContext* labelPrimary();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LabelFactorContext* labelFactor();

  class LabelPrimaryContext : public antlr4::ParserRuleContext {
   public:
    LabelPrimaryContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    LabelPrimaryContext() = default;
    void copyFrom(LabelPrimaryContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class ParenthesizedLabelExpressionContext : public LabelPrimaryContext {
   public:
    ParenthesizedLabelExpressionContext(LabelPrimaryContext* ctx);

    LabelExpressionContext* labelExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LabelWildcardContext : public LabelPrimaryContext {
   public:
    LabelWildcardContext(LabelPrimaryContext* ctx);

    antlr4::tree::TerminalNode* PERCENT();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LabelNameContext : public LabelPrimaryContext {
   public:
    LabelNameContext(LabelPrimaryContext* ctx);

    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LabelPrimaryContext* labelPrimary();

  class GraphQuantifierContext : public antlr4::ParserRuleContext {
   public:
    GraphQuantifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    GraphQuantifierContext() = default;
    void copyFrom(GraphQuantifierContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class OneOrMoreContext : public GraphQuantifierContext {
   public:
    OneOrMoreContext(GraphQuantifierContext* ctx);

    antlr4::tree::TerminalNode* PLUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ZeroOrMoreContext : public GraphQuantifierContext {
   public:
    ZeroOrMoreContext(GraphQuantifierContext* ctx);

    antlr4::tree::TerminalNode* ASTERISK();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ExactRepetitionsContext : public GraphQuantifierContext {
   public:
    ExactRepetitionsContext(GraphQuantifierContext* ctx);

    antlr4::Token* exact = nullptr;
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class BoundedRepetitionsContext : public GraphQuantifierContext {
   public:
    BoundedRepetitionsContext(GraphQuantifierContext* ctx);

    antlr4::Token* lower = nullptr;
    antlr4::Token* upper = nullptr;
    std::vector<antlr4::tree::TerminalNode*> INTEGER_VALUE();
    antlr4::tree::TerminalNode* INTEGER_VALUE(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphQuantifierContext* graphQuantifier();

  class ExpressionContext : public antlr4::ParserRuleContext {
   public:
    ExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BooleanExpressionContext* booleanExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ExpressionContext* expression();

  class BooleanExpressionContext : public antlr4::ParserRuleContext {
   public:
    BooleanExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    BooleanExpressionContext() = default;
    void copyFrom(BooleanExpressionContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class LogicalNotContext : public BooleanExpressionContext {
   public:
    LogicalNotContext(BooleanExpressionContext* ctx);

    antlr4::tree::TerminalNode* NOT();
    BooleanExpressionContext* booleanExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class PredicatedContext : public BooleanExpressionContext {
   public:
    PredicatedContext(BooleanExpressionContext* ctx);

    PrestoSqlParser::ValueExpressionContext* valueExpressionContext = nullptr;
    ValueExpressionContext* valueExpression();
    PredicateContext* predicate();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LogicalBinaryContext : public BooleanExpressionContext {
   public:
    LogicalBinaryContext(BooleanExpressionContext* ctx);

    PrestoSqlParser::BooleanExpressionContext* left = nullptr;
    antlr4::Token* op = nullptr;
    PrestoSqlParser::BooleanExpressionContext* right = nullptr;
    std::vector<BooleanExpressionContext*> booleanExpression();
    BooleanExpressionContext* booleanExpression(size_t i);
    antlr4::tree::TerminalNode* AND();
    antlr4::tree::TerminalNode* OR();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BooleanExpressionContext* booleanExpression();
  BooleanExpressionContext* booleanExpression(int precedence);
  class PredicateContext : public antlr4::ParserRuleContext {
   public:
    antlr4::ParserRuleContext* value;
    PredicateContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    PredicateContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState,
        antlr4::ParserRuleContext* value);

    PredicateContext() = default;
    void copyFrom(PredicateContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class ComparisonContext : public PredicateContext {
   public:
    ComparisonContext(PredicateContext* ctx);

    PrestoSqlParser::ValueExpressionContext* right = nullptr;
    ComparisonOperatorContext* comparisonOperator();
    ValueExpressionContext* valueExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LikeContext : public PredicateContext {
   public:
    LikeContext(PredicateContext* ctx);

    PrestoSqlParser::ValueExpressionContext* pattern = nullptr;
    PrestoSqlParser::ValueExpressionContext* escape = nullptr;
    antlr4::tree::TerminalNode* LIKE();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* ESCAPE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class InSubqueryContext : public PredicateContext {
   public:
    InSubqueryContext(PredicateContext* ctx);

    antlr4::tree::TerminalNode* IN();
    QueryContext* query();
    antlr4::tree::TerminalNode* NOT();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DistinctFromContext : public PredicateContext {
   public:
    DistinctFromContext(PredicateContext* ctx);

    PrestoSqlParser::ValueExpressionContext* right = nullptr;
    antlr4::tree::TerminalNode* IS();
    antlr4::tree::TerminalNode* DISTINCT();
    antlr4::tree::TerminalNode* FROM();
    ValueExpressionContext* valueExpression();
    antlr4::tree::TerminalNode* NOT();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class InListContext : public PredicateContext {
   public:
    InListContext(PredicateContext* ctx);

    antlr4::tree::TerminalNode* IN();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* NOT();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class NullPredicateContext : public PredicateContext {
   public:
    NullPredicateContext(PredicateContext* ctx);

    antlr4::tree::TerminalNode* IS();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    antlr4::tree::TerminalNode* NOT();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class BetweenContext : public PredicateContext {
   public:
    BetweenContext(PredicateContext* ctx);

    PrestoSqlParser::ValueExpressionContext* lower = nullptr;
    PrestoSqlParser::ValueExpressionContext* upper = nullptr;
    antlr4::tree::TerminalNode* BETWEEN();
    antlr4::tree::TerminalNode* AND();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* NOT();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class QuantifiedComparisonContext : public PredicateContext {
   public:
    QuantifiedComparisonContext(PredicateContext* ctx);

    ComparisonOperatorContext* comparisonOperator();
    ComparisonQuantifierContext* comparisonQuantifier();
    QueryContext* query();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PredicateContext* predicate(antlr4::ParserRuleContext* value);

  class ValueExpressionContext : public antlr4::ParserRuleContext {
   public:
    ValueExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    ValueExpressionContext() = default;
    void copyFrom(ValueExpressionContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class ValueExpressionDefaultContext : public ValueExpressionContext {
   public:
    ValueExpressionDefaultContext(ValueExpressionContext* ctx);

    PrimaryExpressionContext* primaryExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ConcatenationContext : public ValueExpressionContext {
   public:
    ConcatenationContext(ValueExpressionContext* ctx);

    PrestoSqlParser::ValueExpressionContext* left = nullptr;
    PrestoSqlParser::ValueExpressionContext* right = nullptr;
    antlr4::tree::TerminalNode* CONCAT();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ArithmeticBinaryContext : public ValueExpressionContext {
   public:
    ArithmeticBinaryContext(ValueExpressionContext* ctx);

    PrestoSqlParser::ValueExpressionContext* left = nullptr;
    antlr4::Token* op = nullptr;
    PrestoSqlParser::ValueExpressionContext* right = nullptr;
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* ASTERISK();
    antlr4::tree::TerminalNode* SLASH();
    antlr4::tree::TerminalNode* PERCENT();
    antlr4::tree::TerminalNode* PLUS();
    antlr4::tree::TerminalNode* MINUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ArithmeticUnaryContext : public ValueExpressionContext {
   public:
    ArithmeticUnaryContext(ValueExpressionContext* ctx);

    antlr4::Token* op = nullptr;
    ValueExpressionContext* valueExpression();
    antlr4::tree::TerminalNode* MINUS();
    antlr4::tree::TerminalNode* PLUS();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AtTimeZoneContext : public ValueExpressionContext {
   public:
    AtTimeZoneContext(ValueExpressionContext* ctx);

    ValueExpressionContext* valueExpression();
    antlr4::tree::TerminalNode* AT();
    TimeZoneSpecifierContext* timeZoneSpecifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ValueExpressionContext* valueExpression();
  ValueExpressionContext* valueExpression(int precedence);
  class PrimaryExpressionContext : public antlr4::ParserRuleContext {
   public:
    PrimaryExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    PrimaryExpressionContext() = default;
    void copyFrom(PrimaryExpressionContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class DereferenceContext : public PrimaryExpressionContext {
   public:
    DereferenceContext(PrimaryExpressionContext* ctx);

    PrestoSqlParser::PrimaryExpressionContext* base = nullptr;
    PrestoSqlParser::IdentifierContext* fieldName = nullptr;
    PrimaryExpressionContext* primaryExpression();
    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TypeConstructorContext : public PrimaryExpressionContext {
   public:
    TypeConstructorContext(PrimaryExpressionContext* ctx);

    TypeContext* type();
    StringContext* string();
    antlr4::tree::TerminalNode* DOUBLE_PRECISION();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SpecialDateTimeFunctionContext : public PrimaryExpressionContext {
   public:
    SpecialDateTimeFunctionContext(PrimaryExpressionContext* ctx);

    antlr4::Token* name = nullptr;
    antlr4::Token* precision = nullptr;
    antlr4::tree::TerminalNode* CURRENT_DATE();
    antlr4::tree::TerminalNode* CURRENT_TIME();
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    antlr4::tree::TerminalNode* CURRENT_TIMESTAMP();
    antlr4::tree::TerminalNode* LOCALTIME();
    antlr4::tree::TerminalNode* LOCALTIMESTAMP();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SubstringContext : public PrimaryExpressionContext {
   public:
    SubstringContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* SUBSTRING();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* FOR();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CastContext : public PrimaryExpressionContext {
   public:
    CastContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* CAST();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* AS();
    TypeContext* type();
    antlr4::tree::TerminalNode* TRY_CAST();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LambdaContext : public PrimaryExpressionContext {
   public:
    LambdaContext(PrimaryExpressionContext* ctx);

    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    ExpressionContext* expression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ParenthesizedExpressionContext : public PrimaryExpressionContext {
   public:
    ParenthesizedExpressionContext(PrimaryExpressionContext* ctx);

    ExpressionContext* expression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ParameterContext : public PrimaryExpressionContext {
   public:
    ParameterContext(PrimaryExpressionContext* ctx);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class NormalizeContext : public PrimaryExpressionContext {
   public:
    NormalizeContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* NORMALIZE();
    ValueExpressionContext* valueExpression();
    NormalFormContext* normalForm();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class IntervalLiteralContext : public PrimaryExpressionContext {
   public:
    IntervalLiteralContext(PrimaryExpressionContext* ctx);

    IntervalContext* interval();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class NumericLiteralContext : public PrimaryExpressionContext {
   public:
    NumericLiteralContext(PrimaryExpressionContext* ctx);

    NumberContext* number();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class BooleanLiteralContext : public PrimaryExpressionContext {
   public:
    BooleanLiteralContext(PrimaryExpressionContext* ctx);

    BooleanValueContext* booleanValue();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SimpleCaseContext : public PrimaryExpressionContext {
   public:
    SimpleCaseContext(PrimaryExpressionContext* ctx);

    PrestoSqlParser::ExpressionContext* elseExpression = nullptr;
    antlr4::tree::TerminalNode* CASE();
    ValueExpressionContext* valueExpression();
    antlr4::tree::TerminalNode* END();
    std::vector<WhenClauseContext*> whenClause();
    WhenClauseContext* whenClause(size_t i);
    antlr4::tree::TerminalNode* ELSE();
    ExpressionContext* expression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ColumnReferenceContext : public PrimaryExpressionContext {
   public:
    ColumnReferenceContext(PrimaryExpressionContext* ctx);

    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class NullLiteralContext : public PrimaryExpressionContext {
   public:
    NullLiteralContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* NULL_LITERAL();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RowConstructorContext : public PrimaryExpressionContext {
   public:
    RowConstructorContext(PrimaryExpressionContext* ctx);

    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* ROW();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class NamedRowConstructorContext : public PrimaryExpressionContext {
   public:
    NamedRowConstructorContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* ROW();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> AS();
    antlr4::tree::TerminalNode* AS(size_t i);
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SubscriptContext : public PrimaryExpressionContext {
   public:
    SubscriptContext(PrimaryExpressionContext* ctx);

    PrestoSqlParser::PrimaryExpressionContext* value = nullptr;
    PrestoSqlParser::ValueExpressionContext* index = nullptr;
    PrimaryExpressionContext* primaryExpression();
    ValueExpressionContext* valueExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SubqueryExpressionContext : public PrimaryExpressionContext {
   public:
    SubqueryExpressionContext(PrimaryExpressionContext* ctx);

    QueryContext* query();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class BinaryLiteralContext : public PrimaryExpressionContext {
   public:
    BinaryLiteralContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* BINARY_LITERAL();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CurrentUserContext : public PrimaryExpressionContext {
   public:
    CurrentUserContext(PrimaryExpressionContext* ctx);

    antlr4::Token* name = nullptr;
    antlr4::tree::TerminalNode* CURRENT_USER();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ExtractContext : public PrimaryExpressionContext {
   public:
    ExtractContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* EXTRACT();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* FROM();
    ValueExpressionContext* valueExpression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class StringLiteralContext : public PrimaryExpressionContext {
   public:
    StringLiteralContext(PrimaryExpressionContext* ctx);

    StringContext* string();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ArrayConstructorContext : public PrimaryExpressionContext {
   public:
    ArrayConstructorContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* ARRAY();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class FunctionCallContext : public PrimaryExpressionContext {
   public:
    FunctionCallContext(PrimaryExpressionContext* ctx);

    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* ASTERISK();
    FilterContext* filter();
    OverContext* over();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* ORDER();
    antlr4::tree::TerminalNode* BY();
    std::vector<SortItemContext*> sortItem();
    SortItemContext* sortItem(size_t i);
    SetQuantifierContext* setQuantifier();
    NullTreatmentContext* nullTreatment();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ExistsContext : public PrimaryExpressionContext {
   public:
    ExistsContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* EXISTS();
    QueryContext* query();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class PositionContext : public PrimaryExpressionContext {
   public:
    PositionContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* POSITION();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* IN();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SearchedCaseContext : public PrimaryExpressionContext {
   public:
    SearchedCaseContext(PrimaryExpressionContext* ctx);

    PrestoSqlParser::ExpressionContext* elseExpression = nullptr;
    antlr4::tree::TerminalNode* CASE();
    antlr4::tree::TerminalNode* END();
    std::vector<WhenClauseContext*> whenClause();
    WhenClauseContext* whenClause(size_t i);
    antlr4::tree::TerminalNode* ELSE();
    ExpressionContext* expression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class GroupingOperationContext : public PrimaryExpressionContext {
   public:
    GroupingOperationContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* GROUPING();
    std::vector<QualifiedNameContext*> qualifiedName();
    QualifiedNameContext* qualifiedName(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class MethodCallContext : public PrimaryExpressionContext {
   public:
    MethodCallContext(PrimaryExpressionContext* ctx);

    PrestoSqlParser::PrimaryExpressionContext* base = nullptr;
    PrestoSqlParser::IdentifierContext* functionName = nullptr;
    PrestoSqlParser::ExpressionContext* expressionContext = nullptr;
    std::vector<ExpressionContext*> arguments;
    PrimaryExpressionContext* primaryExpression();
    IdentifierContext* identifier();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PrimaryExpressionContext* primaryExpression();
  PrimaryExpressionContext* primaryExpression(int precedence);
  class StringContext : public antlr4::ParserRuleContext {
   public:
    StringContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    StringContext() = default;
    void copyFrom(StringContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class UnicodeStringLiteralContext : public StringContext {
   public:
    UnicodeStringLiteralContext(StringContext* ctx);

    antlr4::tree::TerminalNode* UNICODE_STRING();
    antlr4::tree::TerminalNode* UESCAPE();
    antlr4::tree::TerminalNode* STRING();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class BasicStringLiteralContext : public StringContext {
   public:
    BasicStringLiteralContext(StringContext* ctx);

    antlr4::tree::TerminalNode* STRING();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StringContext* string();

  class NullTreatmentContext : public antlr4::ParserRuleContext {
   public:
    NullTreatmentContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* IGNORE();
    antlr4::tree::TerminalNode* NULLS();
    antlr4::tree::TerminalNode* RESPECT();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NullTreatmentContext* nullTreatment();

  class TimeZoneSpecifierContext : public antlr4::ParserRuleContext {
   public:
    TimeZoneSpecifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    TimeZoneSpecifierContext() = default;
    void copyFrom(TimeZoneSpecifierContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class TimeZoneIntervalContext : public TimeZoneSpecifierContext {
   public:
    TimeZoneIntervalContext(TimeZoneSpecifierContext* ctx);

    antlr4::tree::TerminalNode* TIME();
    antlr4::tree::TerminalNode* ZONE();
    IntervalContext* interval();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TimeZoneStringContext : public TimeZoneSpecifierContext {
   public:
    TimeZoneStringContext(TimeZoneSpecifierContext* ctx);

    antlr4::tree::TerminalNode* TIME();
    antlr4::tree::TerminalNode* ZONE();
    StringContext* string();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TimeZoneSpecifierContext* timeZoneSpecifier();

  class ComparisonOperatorContext : public antlr4::ParserRuleContext {
   public:
    ComparisonOperatorContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* EQ();
    antlr4::tree::TerminalNode* NEQ();
    antlr4::tree::TerminalNode* LT();
    antlr4::tree::TerminalNode* LTE();
    antlr4::tree::TerminalNode* GT();
    antlr4::tree::TerminalNode* GTE();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ComparisonOperatorContext* comparisonOperator();

  class ComparisonQuantifierContext : public antlr4::ParserRuleContext {
   public:
    ComparisonQuantifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ALL();
    antlr4::tree::TerminalNode* SOME();
    antlr4::tree::TerminalNode* ANY();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ComparisonQuantifierContext* comparisonQuantifier();

  class BooleanValueContext : public antlr4::ParserRuleContext {
   public:
    BooleanValueContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* TRUE();
    antlr4::tree::TerminalNode* FALSE();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BooleanValueContext* booleanValue();

  class IntervalContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* sign = nullptr;
    PrestoSqlParser::IntervalFieldContext* from = nullptr;
    PrestoSqlParser::IntervalFieldContext* to = nullptr;
    IntervalContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTERVAL();
    StringContext* string();
    std::vector<IntervalFieldContext*> intervalField();
    IntervalFieldContext* intervalField(size_t i);
    antlr4::tree::TerminalNode* TO();
    antlr4::tree::TerminalNode* PLUS();
    antlr4::tree::TerminalNode* MINUS();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IntervalContext* interval();

  class IntervalFieldContext : public antlr4::ParserRuleContext {
   public:
    IntervalFieldContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* YEAR();
    antlr4::tree::TerminalNode* MONTH();
    antlr4::tree::TerminalNode* DAY();
    antlr4::tree::TerminalNode* HOUR();
    antlr4::tree::TerminalNode* MINUTE();
    antlr4::tree::TerminalNode* SECOND();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IntervalFieldContext* intervalField();

  class NormalFormContext : public antlr4::ParserRuleContext {
   public:
    NormalFormContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NFD();
    antlr4::tree::TerminalNode* NFC();
    antlr4::tree::TerminalNode* NFKD();
    antlr4::tree::TerminalNode* NFKC();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NormalFormContext* normalForm();

  class TypesContext : public antlr4::ParserRuleContext {
   public:
    TypesContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TypeContext*> type();
    TypeContext* type(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TypesContext* types();

  class TypeContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IntervalFieldContext* from = nullptr;
    PrestoSqlParser::IntervalFieldContext* to = nullptr;
    TypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ARRAY();
    antlr4::tree::TerminalNode* LT();
    std::vector<TypeContext*> type();
    TypeContext* type(size_t i);
    antlr4::tree::TerminalNode* GT();
    antlr4::tree::TerminalNode* MAP();
    antlr4::tree::TerminalNode* ROW();
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    BaseTypeContext* baseType();
    std::vector<TypeParameterContext*> typeParameter();
    TypeParameterContext* typeParameter(size_t i);
    antlr4::tree::TerminalNode* INTERVAL();
    antlr4::tree::TerminalNode* TO();
    std::vector<IntervalFieldContext*> intervalField();
    IntervalFieldContext* intervalField(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TypeContext* type();
  TypeContext* type(int precedence);
  class TypeParameterContext : public antlr4::ParserRuleContext {
   public:
    TypeParameterContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    TypeContext* type();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TypeParameterContext* typeParameter();

  class BaseTypeContext : public antlr4::ParserRuleContext {
   public:
    BaseTypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* TIME_WITH_TIME_ZONE();
    antlr4::tree::TerminalNode* TIMESTAMP_WITH_TIME_ZONE();
    antlr4::tree::TerminalNode* DOUBLE_PRECISION();
    QualifiedNameContext* qualifiedName();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BaseTypeContext* baseType();

  class WhenClauseContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::ExpressionContext* condition = nullptr;
    PrestoSqlParser::ExpressionContext* result = nullptr;
    WhenClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WHEN();
    antlr4::tree::TerminalNode* THEN();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WhenClauseContext* whenClause();

  class FilterContext : public antlr4::ParserRuleContext {
   public:
    FilterContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* FILTER();
    antlr4::tree::TerminalNode* WHERE();
    BooleanExpressionContext* booleanExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FilterContext* filter();

  class OverContext : public antlr4::ParserRuleContext {
   public:
    OverContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* OVER();
    IdentifierContext* identifier();
    WindowSpecificationContext* windowSpecification();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  OverContext* over();

  class WindowSpecificationContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* existingWindowName = nullptr;
    PrestoSqlParser::ExpressionContext* expressionContext = nullptr;
    std::vector<ExpressionContext*> partition;
    WindowSpecificationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* PARTITION();
    std::vector<antlr4::tree::TerminalNode*> BY();
    antlr4::tree::TerminalNode* BY(size_t i);
    antlr4::tree::TerminalNode* ORDER();
    std::vector<SortItemContext*> sortItem();
    SortItemContext* sortItem(size_t i);
    WindowFrameContext* windowFrame();
    IdentifierContext* identifier();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WindowSpecificationContext* windowSpecification();

  class WindowFrameContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* frameType = nullptr;
    PrestoSqlParser::FrameBoundContext* start = nullptr;
    PrestoSqlParser::FrameBoundContext* end = nullptr;
    WindowFrameContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* RANGE();
    std::vector<FrameBoundContext*> frameBound();
    FrameBoundContext* frameBound(size_t i);
    antlr4::tree::TerminalNode* ROWS();
    antlr4::tree::TerminalNode* GROUPS();
    antlr4::tree::TerminalNode* BETWEEN();
    antlr4::tree::TerminalNode* AND();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WindowFrameContext* windowFrame();

  class FrameBoundContext : public antlr4::ParserRuleContext {
   public:
    FrameBoundContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    FrameBoundContext() = default;
    void copyFrom(FrameBoundContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class BoundedFrameContext : public FrameBoundContext {
   public:
    BoundedFrameContext(FrameBoundContext* ctx);

    antlr4::Token* boundType = nullptr;
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* PRECEDING();
    antlr4::tree::TerminalNode* FOLLOWING();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UnboundedFrameContext : public FrameBoundContext {
   public:
    UnboundedFrameContext(FrameBoundContext* ctx);

    antlr4::Token* boundType = nullptr;
    antlr4::tree::TerminalNode* UNBOUNDED();
    antlr4::tree::TerminalNode* PRECEDING();
    antlr4::tree::TerminalNode* FOLLOWING();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CurrentRowBoundContext : public FrameBoundContext {
   public:
    CurrentRowBoundContext(FrameBoundContext* ctx);

    antlr4::tree::TerminalNode* CURRENT();
    antlr4::tree::TerminalNode* ROW();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FrameBoundContext* frameBound();

  class UpdateAssignmentContext : public antlr4::ParserRuleContext {
   public:
    UpdateAssignmentContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* EQ();
    ExpressionContext* expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UpdateAssignmentContext* updateAssignment();

  class ExplainOptionContext : public antlr4::ParserRuleContext {
   public:
    ExplainOptionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    ExplainOptionContext() = default;
    void copyFrom(ExplainOptionContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class ExplainFormatContext : public ExplainOptionContext {
   public:
    ExplainFormatContext(ExplainOptionContext* ctx);

    antlr4::Token* value = nullptr;
    antlr4::tree::TerminalNode* FORMAT();
    antlr4::tree::TerminalNode* TEXT();
    antlr4::tree::TerminalNode* GRAPHVIZ();
    antlr4::tree::TerminalNode* JSON();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ExplainTypeContext : public ExplainOptionContext {
   public:
    ExplainTypeContext(ExplainOptionContext* ctx);

    antlr4::Token* value = nullptr;
    antlr4::tree::TerminalNode* TYPE();
    antlr4::tree::TerminalNode* LOGICAL();
    antlr4::tree::TerminalNode* GRAPH();
    antlr4::tree::TerminalNode* OPTIMIZED();
    antlr4::tree::TerminalNode* EXECUTABLE();
    antlr4::tree::TerminalNode* DISTRIBUTED();
    antlr4::tree::TerminalNode* VALIDATE();
    antlr4::tree::TerminalNode* IO();
    antlr4::tree::TerminalNode* WITH();
    PropertiesContext* properties();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ExplainOptionContext* explainOption();

  class TransactionModeContext : public antlr4::ParserRuleContext {
   public:
    TransactionModeContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    TransactionModeContext() = default;
    void copyFrom(TransactionModeContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class TransactionAccessModeContext : public TransactionModeContext {
   public:
    TransactionAccessModeContext(TransactionModeContext* ctx);

    antlr4::Token* accessMode = nullptr;
    antlr4::tree::TerminalNode* READ();
    antlr4::tree::TerminalNode* ONLY();
    antlr4::tree::TerminalNode* WRITE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class IsolationLevelContext : public TransactionModeContext {
   public:
    IsolationLevelContext(TransactionModeContext* ctx);

    antlr4::tree::TerminalNode* ISOLATION();
    antlr4::tree::TerminalNode* LEVEL();
    LevelOfIsolationContext* levelOfIsolation();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TransactionModeContext* transactionMode();

  class LevelOfIsolationContext : public antlr4::ParserRuleContext {
   public:
    LevelOfIsolationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    LevelOfIsolationContext() = default;
    void copyFrom(LevelOfIsolationContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class ReadUncommittedContext : public LevelOfIsolationContext {
   public:
    ReadUncommittedContext(LevelOfIsolationContext* ctx);

    antlr4::tree::TerminalNode* READ();
    antlr4::tree::TerminalNode* UNCOMMITTED();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SerializableContext : public LevelOfIsolationContext {
   public:
    SerializableContext(LevelOfIsolationContext* ctx);

    antlr4::tree::TerminalNode* SERIALIZABLE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ReadCommittedContext : public LevelOfIsolationContext {
   public:
    ReadCommittedContext(LevelOfIsolationContext* ctx);

    antlr4::tree::TerminalNode* READ();
    antlr4::tree::TerminalNode* COMMITTED();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RepeatableReadContext : public LevelOfIsolationContext {
   public:
    RepeatableReadContext(LevelOfIsolationContext* ctx);

    antlr4::tree::TerminalNode* REPEATABLE();
    antlr4::tree::TerminalNode* READ();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LevelOfIsolationContext* levelOfIsolation();

  class CallArgumentContext : public antlr4::ParserRuleContext {
   public:
    CallArgumentContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    CallArgumentContext() = default;
    void copyFrom(CallArgumentContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class PositionalArgumentContext : public CallArgumentContext {
   public:
    PositionalArgumentContext(CallArgumentContext* ctx);

    ExpressionContext* expression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class NamedArgumentContext : public CallArgumentContext {
   public:
    NamedArgumentContext(CallArgumentContext* ctx);

    IdentifierContext* identifier();
    ExpressionContext* expression();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CallArgumentContext* callArgument();

  class PrivilegeContext : public antlr4::ParserRuleContext {
   public:
    PrivilegeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SELECT();
    antlr4::tree::TerminalNode* DELETE();
    antlr4::tree::TerminalNode* INSERT();
    IdentifierContext* identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PrivilegeContext* privilege();

  class QualifiedNameContext : public antlr4::ParserRuleContext {
   public:
    QualifiedNameContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QualifiedNameContext* qualifiedName();

  class TableVersionExpressionContext : public antlr4::ParserRuleContext {
   public:
    TableVersionExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    TableVersionExpressionContext() = default;
    void copyFrom(TableVersionExpressionContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class TableVersionContext : public TableVersionExpressionContext {
   public:
    TableVersionContext(TableVersionExpressionContext* ctx);

    antlr4::Token* tableVersionType = nullptr;
    antlr4::tree::TerminalNode* FOR();
    TableVersionStateContext* tableVersionState();
    ValueExpressionContext* valueExpression();
    antlr4::tree::TerminalNode* SYSTEM_TIME();
    antlr4::tree::TerminalNode* SYSTEM_VERSION();
    antlr4::tree::TerminalNode* TIMESTAMP();
    antlr4::tree::TerminalNode* VERSION();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TableVersionExpressionContext* tableVersionExpression();

  class TableVersionStateContext : public antlr4::ParserRuleContext {
   public:
    TableVersionStateContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    TableVersionStateContext() = default;
    void copyFrom(TableVersionStateContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class TableversionbeforeContext : public TableVersionStateContext {
   public:
    TableversionbeforeContext(TableVersionStateContext* ctx);

    antlr4::tree::TerminalNode* BEFORE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TableversionasofContext : public TableVersionStateContext {
   public:
    TableversionasofContext(TableVersionStateContext* ctx);

    antlr4::tree::TerminalNode* AS();
    antlr4::tree::TerminalNode* OF();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TableVersionStateContext* tableVersionState();

  class GrantorContext : public antlr4::ParserRuleContext {
   public:
    GrantorContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    GrantorContext() = default;
    void copyFrom(GrantorContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class CurrentUserGrantorContext : public GrantorContext {
   public:
    CurrentUserGrantorContext(GrantorContext* ctx);

    antlr4::tree::TerminalNode* CURRENT_USER();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SpecifiedPrincipalContext : public GrantorContext {
   public:
    SpecifiedPrincipalContext(GrantorContext* ctx);

    PrincipalContext* principal();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CurrentRoleGrantorContext : public GrantorContext {
   public:
    CurrentRoleGrantorContext(GrantorContext* ctx);

    antlr4::tree::TerminalNode* CURRENT_ROLE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GrantorContext* grantor();

  class PrincipalContext : public antlr4::ParserRuleContext {
   public:
    PrincipalContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    PrincipalContext() = default;
    void copyFrom(PrincipalContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class UnspecifiedPrincipalContext : public PrincipalContext {
   public:
    UnspecifiedPrincipalContext(PrincipalContext* ctx);

    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UserPrincipalContext : public PrincipalContext {
   public:
    UserPrincipalContext(PrincipalContext* ctx);

    antlr4::tree::TerminalNode* USER();
    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RolePrincipalContext : public PrincipalContext {
   public:
    RolePrincipalContext(PrincipalContext* ctx);

    antlr4::tree::TerminalNode* ROLE();
    IdentifierContext* identifier();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PrincipalContext* principal();

  class RolesContext : public antlr4::ParserRuleContext {
   public:
    RolesContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RolesContext* roles();

  class IdentifierContext : public antlr4::ParserRuleContext {
   public:
    IdentifierContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    IdentifierContext() = default;
    void copyFrom(IdentifierContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class BackQuotedIdentifierContext : public IdentifierContext {
   public:
    BackQuotedIdentifierContext(IdentifierContext* ctx);

    antlr4::tree::TerminalNode* BACKQUOTED_IDENTIFIER();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class QuotedIdentifierContext : public IdentifierContext {
   public:
    QuotedIdentifierContext(IdentifierContext* ctx);

    antlr4::tree::TerminalNode* QUOTED_IDENTIFIER();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DigitIdentifierContext : public IdentifierContext {
   public:
    DigitIdentifierContext(IdentifierContext* ctx);

    antlr4::tree::TerminalNode* DIGIT_IDENTIFIER();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UnquotedIdentifierContext : public IdentifierContext {
   public:
    UnquotedIdentifierContext(IdentifierContext* ctx);

    antlr4::tree::TerminalNode* IDENTIFIER();
    NonReservedContext* nonReserved();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IdentifierContext* identifier();

  class NumberContext : public antlr4::ParserRuleContext {
   public:
    NumberContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    NumberContext() = default;
    void copyFrom(NumberContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class DecimalLiteralContext : public NumberContext {
   public:
    DecimalLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* DECIMAL_VALUE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DoubleLiteralContext : public NumberContext {
   public:
    DoubleLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* DOUBLE_VALUE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class IntegerLiteralContext : public NumberContext {
   public:
    IntegerLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* INTEGER_VALUE();
    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NumberContext* number();

  class ConstraintSpecificationContext : public antlr4::ParserRuleContext {
   public:
    ConstraintSpecificationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NamedConstraintSpecificationContext* namedConstraintSpecification();
    UnnamedConstraintSpecificationContext* unnamedConstraintSpecification();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstraintSpecificationContext* constraintSpecification();

  class NamedConstraintSpecificationContext : public antlr4::ParserRuleContext {
   public:
    PrestoSqlParser::IdentifierContext* name = nullptr;
    NamedConstraintSpecificationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* CONSTRAINT();
    UnnamedConstraintSpecificationContext* unnamedConstraintSpecification();
    IdentifierContext* identifier();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NamedConstraintSpecificationContext* namedConstraintSpecification();

  class UnnamedConstraintSpecificationContext
      : public antlr4::ParserRuleContext {
   public:
    UnnamedConstraintSpecificationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ConstraintTypeContext* constraintType();
    ColumnAliasesContext* columnAliases();
    ConstraintQualifiersContext* constraintQualifiers();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnnamedConstraintSpecificationContext* unnamedConstraintSpecification();

  class ConstraintTypeContext : public antlr4::ParserRuleContext {
   public:
    ConstraintTypeContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* UNIQUE();
    antlr4::tree::TerminalNode* PRIMARY();
    antlr4::tree::TerminalNode* KEY();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstraintTypeContext* constraintType();

  class ConstraintQualifiersContext : public antlr4::ParserRuleContext {
   public:
    ConstraintQualifiersContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ConstraintQualifierContext*> constraintQualifier();
    ConstraintQualifierContext* constraintQualifier(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstraintQualifiersContext* constraintQualifiers();

  class ConstraintQualifierContext : public antlr4::ParserRuleContext {
   public:
    ConstraintQualifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ConstraintEnabledContext* constraintEnabled();
    ConstraintRelyContext* constraintRely();
    ConstraintEnforcedContext* constraintEnforced();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstraintQualifierContext* constraintQualifier();

  class ConstraintRelyContext : public antlr4::ParserRuleContext {
   public:
    ConstraintRelyContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* RELY();
    antlr4::tree::TerminalNode* NOT();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstraintRelyContext* constraintRely();

  class ConstraintEnabledContext : public antlr4::ParserRuleContext {
   public:
    ConstraintEnabledContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ENABLED();
    antlr4::tree::TerminalNode* DISABLED();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstraintEnabledContext* constraintEnabled();

  class ConstraintEnforcedContext : public antlr4::ParserRuleContext {
   public:
    ConstraintEnforcedContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ENFORCED();
    antlr4::tree::TerminalNode* NOT();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstraintEnforcedContext* constraintEnforced();

  class NonReservedContext : public antlr4::ParserRuleContext {
   public:
    NonReservedContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ACYCLIC();
    antlr4::tree::TerminalNode* ADD();
    antlr4::tree::TerminalNode* ADMIN();
    antlr4::tree::TerminalNode* ALL();
    antlr4::tree::TerminalNode* ANALYZE();
    antlr4::tree::TerminalNode* ANY();
    antlr4::tree::TerminalNode* ARRAY();
    antlr4::tree::TerminalNode* ASC();
    antlr4::tree::TerminalNode* AT();
    antlr4::tree::TerminalNode* BEFORE();
    antlr4::tree::TerminalNode* BERNOULLI();
    antlr4::tree::TerminalNode* CALL();
    antlr4::tree::TerminalNode* CALLED();
    antlr4::tree::TerminalNode* CASCADE();
    antlr4::tree::TerminalNode* CATALOGS();
    antlr4::tree::TerminalNode* COLUMN();
    antlr4::tree::TerminalNode* COLUMNS();
    antlr4::tree::TerminalNode* COMMENT();
    antlr4::tree::TerminalNode* COMMIT();
    antlr4::tree::TerminalNode* COMMITTED();
    antlr4::tree::TerminalNode* CURRENT();
    antlr4::tree::TerminalNode* CURRENT_ROLE();
    antlr4::tree::TerminalNode* DATA();
    antlr4::tree::TerminalNode* DATE();
    antlr4::tree::TerminalNode* DAY();
    antlr4::tree::TerminalNode* DEFINER();
    antlr4::tree::TerminalNode* DESC();
    antlr4::tree::TerminalNode* DESTINATION();
    antlr4::tree::TerminalNode* DETERMINISTIC();
    antlr4::tree::TerminalNode* DISABLED();
    antlr4::tree::TerminalNode* DISTRIBUTED();
    antlr4::tree::TerminalNode* EDGE();
    antlr4::tree::TerminalNode* ENABLED();
    antlr4::tree::TerminalNode* ENFORCED();
    antlr4::tree::TerminalNode* EXECUTABLE();
    antlr4::tree::TerminalNode* EXCLUDE();
    antlr4::tree::TerminalNode* EXCLUDING();
    antlr4::tree::TerminalNode* EXPLAIN();
    antlr4::tree::TerminalNode* EXTERNAL();
    antlr4::tree::TerminalNode* FETCH();
    antlr4::tree::TerminalNode* FILTER();
    antlr4::tree::TerminalNode* FIRST();
    antlr4::tree::TerminalNode* FOLLOWING();
    antlr4::tree::TerminalNode* FORMAT();
    antlr4::tree::TerminalNode* FUNCTION();
    antlr4::tree::TerminalNode* FUNCTIONS();
    antlr4::tree::TerminalNode* GRANT();
    antlr4::tree::TerminalNode* GRANTED();
    antlr4::tree::TerminalNode* GRANTS();
    antlr4::tree::TerminalNode* GRAPH();
    antlr4::tree::TerminalNode* GRAPH_TABLE();
    antlr4::tree::TerminalNode* GRAPHVIZ();
    antlr4::tree::TerminalNode* GROUPS();
    antlr4::tree::TerminalNode* HOUR();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* IGNORE();
    antlr4::tree::TerminalNode* INCLUDING();
    antlr4::tree::TerminalNode* INPUT();
    antlr4::tree::TerminalNode* INTERVAL();
    antlr4::tree::TerminalNode* INVOKER();
    antlr4::tree::TerminalNode* IO();
    antlr4::tree::TerminalNode* ISOLATION();
    antlr4::tree::TerminalNode* JSON();
    antlr4::tree::TerminalNode* KEY();
    antlr4::tree::TerminalNode* LABEL();
    antlr4::tree::TerminalNode* LANGUAGE();
    antlr4::tree::TerminalNode* LAST();
    antlr4::tree::TerminalNode* LATERAL();
    antlr4::tree::TerminalNode* LEVEL();
    antlr4::tree::TerminalNode* LIMIT();
    antlr4::tree::TerminalNode* LOGICAL();
    antlr4::tree::TerminalNode* MAP();
    antlr4::tree::TerminalNode* MATCH();
    antlr4::tree::TerminalNode* MATERIALIZED();
    antlr4::tree::TerminalNode* MINUTE();
    antlr4::tree::TerminalNode* MONTH();
    antlr4::tree::TerminalNode* NAME();
    antlr4::tree::TerminalNode* NFC();
    antlr4::tree::TerminalNode* NFD();
    antlr4::tree::TerminalNode* NFKC();
    antlr4::tree::TerminalNode* NFKD();
    antlr4::tree::TerminalNode* NO();
    antlr4::tree::TerminalNode* NONE();
    antlr4::tree::TerminalNode* NULLIF();
    antlr4::tree::TerminalNode* NULLS();
    antlr4::tree::TerminalNode* OF();
    antlr4::tree::TerminalNode* OFFSET();
    antlr4::tree::TerminalNode* ONLY();
    antlr4::tree::TerminalNode* OPTIMIZED();
    antlr4::tree::TerminalNode* OPTION();
    antlr4::tree::TerminalNode* ORDINALITY();
    antlr4::tree::TerminalNode* OUTPUT();
    antlr4::tree::TerminalNode* OVER();
    antlr4::tree::TerminalNode* PARTITION();
    antlr4::tree::TerminalNode* PARTITIONS();
    antlr4::tree::TerminalNode* PATH();
    antlr4::tree::TerminalNode* PATHS();
    antlr4::tree::TerminalNode* POSITION();
    antlr4::tree::TerminalNode* PRECEDING();
    antlr4::tree::TerminalNode* PRIMARY();
    antlr4::tree::TerminalNode* PRIVILEGES();
    antlr4::tree::TerminalNode* PROPERTIES();
    antlr4::tree::TerminalNode* RANGE();
    antlr4::tree::TerminalNode* READ();
    antlr4::tree::TerminalNode* REFERENCES();
    antlr4::tree::TerminalNode* REFRESH();
    antlr4::tree::TerminalNode* RELY();
    antlr4::tree::TerminalNode* RENAME();
    antlr4::tree::TerminalNode* REPEATABLE();
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* RESET();
    antlr4::tree::TerminalNode* RESPECT();
    antlr4::tree::TerminalNode* RESTRICT();
    antlr4::tree::TerminalNode* RETURN();
    antlr4::tree::TerminalNode* RETURNS();
    antlr4::tree::TerminalNode* REVOKE();
    antlr4::tree::TerminalNode* ROLE();
    antlr4::tree::TerminalNode* ROLES();
    antlr4::tree::TerminalNode* ROLLBACK();
    antlr4::tree::TerminalNode* ROW();
    antlr4::tree::TerminalNode* ROWS();
    antlr4::tree::TerminalNode* SCHEMA();
    antlr4::tree::TerminalNode* SCHEMAS();
    antlr4::tree::TerminalNode* SECOND();
    antlr4::tree::TerminalNode* SECURITY();
    antlr4::tree::TerminalNode* SERIALIZABLE();
    antlr4::tree::TerminalNode* SESSION();
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* SETS();
    antlr4::tree::TerminalNode* SQL();
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* SHORTEST();
    antlr4::tree::TerminalNode* SIMPLE();
    antlr4::tree::TerminalNode* SOME();
    antlr4::tree::TerminalNode* SOURCE();
    antlr4::tree::TerminalNode* START();
    antlr4::tree::TerminalNode* STATS();
    antlr4::tree::TerminalNode* SUBSTRING();
    antlr4::tree::TerminalNode* SYSTEM();
    antlr4::tree::TerminalNode* SYSTEM_TIME();
    antlr4::tree::TerminalNode* SYSTEM_VERSION();
    antlr4::tree::TerminalNode* TABLES();
    antlr4::tree::TerminalNode* TABLESAMPLE();
    antlr4::tree::TerminalNode* TEMPORARY();
    antlr4::tree::TerminalNode* TEXT();
    antlr4::tree::TerminalNode* TIME();
    antlr4::tree::TerminalNode* TIMESTAMP();
    antlr4::tree::TerminalNode* TO();
    antlr4::tree::TerminalNode* TRAIL();
    antlr4::tree::TerminalNode* TRANSACTION();
    antlr4::tree::TerminalNode* TRUNCATE();
    antlr4::tree::TerminalNode* TRY_CAST();
    antlr4::tree::TerminalNode* TYPE();
    antlr4::tree::TerminalNode* UNBOUNDED();
    antlr4::tree::TerminalNode* UNCOMMITTED();
    antlr4::tree::TerminalNode* UNIQUE();
    antlr4::tree::TerminalNode* UPDATE();
    antlr4::tree::TerminalNode* USE();
    antlr4::tree::TerminalNode* USER();
    antlr4::tree::TerminalNode* VALIDATE();
    antlr4::tree::TerminalNode* VERBOSE();
    antlr4::tree::TerminalNode* VERSION();
    antlr4::tree::TerminalNode* VERTEX();
    antlr4::tree::TerminalNode* VIEW();
    antlr4::tree::TerminalNode* WALK();
    antlr4::tree::TerminalNode* WINDOW();
    antlr4::tree::TerminalNode* WORK();
    antlr4::tree::TerminalNode* WRITE();
    antlr4::tree::TerminalNode* YEAR();
    antlr4::tree::TerminalNode* ZONE();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NonReservedContext* nonReserved();

  bool sempred(
      antlr4::RuleContext* _localctx,
      size_t ruleIndex,
      size_t predicateIndex) override;

  bool queryTermSempred(QueryTermContext* _localctx, size_t predicateIndex);
  bool relationSempred(RelationContext* _localctx, size_t predicateIndex);
  bool booleanExpressionSempred(
      BooleanExpressionContext* _localctx,
      size_t predicateIndex);
  bool valueExpressionSempred(
      ValueExpressionContext* _localctx,
      size_t predicateIndex);
  bool primaryExpressionSempred(
      PrimaryExpressionContext* _localctx,
      size_t predicateIndex);
  bool typeSempred(TypeContext* _localctx, size_t predicateIndex);

  // By default the static state used to implement the parser is lazily
  // initialized during the first call to the constructor. You can call this
  // function if you wish to initialize the static state ahead of time.
  static void initialize();

 private:
};
