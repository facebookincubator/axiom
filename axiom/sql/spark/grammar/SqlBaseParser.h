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

// Generated from SqlBaseParser.g4 by ANTLR 4.13.2

#pragma once

#include "antlr4-runtime.h"

namespace axiom::sql::spark {

class SqlBaseParser : public antlr4::Parser {
 public:
  enum {
    SEMICOLON = 1,
    LEFT_PAREN = 2,
    RIGHT_PAREN = 3,
    COMMA = 4,
    DOT = 5,
    LEFT_BRACKET = 6,
    RIGHT_BRACKET = 7,
    ADD = 8,
    AFTER = 9,
    ALL = 10,
    ALTER = 11,
    ALWAYS = 12,
    ANALYZE = 13,
    AND = 14,
    ANTI = 15,
    ANY = 16,
    ANY_VALUE = 17,
    ARCHIVE = 18,
    ARRAY = 19,
    AS = 20,
    ASC = 21,
    AT = 22,
    AUTHORIZATION = 23,
    BETWEEN = 24,
    BOTH = 25,
    BUCKET = 26,
    BUCKETS = 27,
    BY = 28,
    CACHE = 29,
    CASCADE = 30,
    CASE = 31,
    CAST = 32,
    CATALOG = 33,
    CATALOGS = 34,
    CHANGE = 35,
    CHECK = 36,
    CLEAR = 37,
    CLUSTER = 38,
    CLUSTERED = 39,
    CODEGEN = 40,
    COLLATE = 41,
    COLLECTION = 42,
    COLUMN = 43,
    COLUMNS = 44,
    COMMENT = 45,
    COMMIT = 46,
    COMPACT = 47,
    COMPACTIONS = 48,
    COMPUTE = 49,
    CONCATENATE = 50,
    CONSTRAINT = 51,
    COST = 52,
    CREATE = 53,
    CROSS = 54,
    CUBE = 55,
    CURRENT = 56,
    CURRENT_DATE = 57,
    CURRENT_TIME = 58,
    CURRENT_TIMESTAMP = 59,
    CURRENT_USER = 60,
    DAY = 61,
    DAYS = 62,
    DAYOFYEAR = 63,
    DATA = 64,
    DATABASE = 65,
    DATABASES = 66,
    DATEADD = 67,
    DATEDIFF = 68,
    DBPROPERTIES = 69,
    DEFAULT = 70,
    DEFINED = 71,
    DELETE = 72,
    DELIMITED = 73,
    DESC = 74,
    DESCRIBE = 75,
    DFS = 76,
    DIRECTORIES = 77,
    DIRECTORY = 78,
    DISTINCT = 79,
    DISTRIBUTE = 80,
    DIV = 81,
    DROP = 82,
    ELSE = 83,
    END = 84,
    ESCAPE = 85,
    ESCAPED = 86,
    EXCEPT = 87,
    EXCHANGE = 88,
    EXCLUDE = 89,
    EXISTS = 90,
    EXPLAIN = 91,
    EXPORT = 92,
    EXTENDED = 93,
    EXTERNAL = 94,
    EXTRACT = 95,
    FALSE = 96,
    FETCH = 97,
    FIELDS = 98,
    FILTER = 99,
    FILEFORMAT = 100,
    FIRST = 101,
    FOLLOWING = 102,
    FOR = 103,
    FOREIGN = 104,
    FORMAT = 105,
    FORMATTED = 106,
    FROM = 107,
    FULL = 108,
    FUNCTION = 109,
    FUNCTIONS = 110,
    GENERATED = 111,
    GLOBAL = 112,
    GRANT = 113,
    GROUP = 114,
    GROUPING = 115,
    HAVING = 116,
    HOUR = 117,
    HOURS = 118,
    IF = 119,
    IGNORE = 120,
    IMPORT = 121,
    IN = 122,
    INCLUDE = 123,
    INDEX = 124,
    INDEXES = 125,
    INNER = 126,
    INPATH = 127,
    INPUTFORMAT = 128,
    INSERT = 129,
    INTERSECT = 130,
    INTERVAL = 131,
    INTO = 132,
    IS = 133,
    ITEMS = 134,
    JOIN = 135,
    KEYS = 136,
    LAST = 137,
    LATERAL = 138,
    LAZY = 139,
    LEADING = 140,
    LEFT = 141,
    LIKE = 142,
    ILIKE = 143,
    LIMIT = 144,
    LINES = 145,
    LIST = 146,
    LOAD = 147,
    LOCAL = 148,
    LOCATION = 149,
    LOCK = 150,
    LOCKS = 151,
    LOGICAL = 152,
    MACRO = 153,
    MAP = 154,
    MATCHED = 155,
    MERGE = 156,
    MICROSECOND = 157,
    MICROSECONDS = 158,
    MILLISECOND = 159,
    MILLISECONDS = 160,
    MINUTE = 161,
    MINUTES = 162,
    MONTH = 163,
    MONTHS = 164,
    MSCK = 165,
    NAMESPACE = 166,
    NAMESPACES = 167,
    NANOSECOND = 168,
    NANOSECONDS = 169,
    NATURAL = 170,
    NO = 171,
    NOT = 172,
    NULL_LITERAL = 173,
    NULLS = 174,
    OF = 175,
    OFFSET = 176,
    ON = 177,
    ONLY = 178,
    OPTION = 179,
    OPTIONS = 180,
    OR = 181,
    ORDER = 182,
    OUT = 183,
    OUTER = 184,
    OUTPUTFORMAT = 185,
    OVER = 186,
    OVERLAPS = 187,
    OVERLAY = 188,
    OVERWRITE = 189,
    PARTITION = 190,
    PARTITIONED = 191,
    PARTITIONS = 192,
    PERCENTILE_CONT = 193,
    PERCENTILE_DISC = 194,
    PERCENTLIT = 195,
    PIVOT = 196,
    PLACING = 197,
    POSITION = 198,
    PRECEDING = 199,
    PRIMARY = 200,
    PRINCIPALS = 201,
    PROPERTIES = 202,
    PURGE = 203,
    QUARTER = 204,
    QUERY = 205,
    RANGE = 206,
    RECORDREADER = 207,
    RECORDWRITER = 208,
    RECOVER = 209,
    REDUCE = 210,
    REFERENCES = 211,
    REFRESH = 212,
    RENAME = 213,
    REPAIR = 214,
    REPEATABLE = 215,
    REPLACE = 216,
    RESET = 217,
    RESPECT = 218,
    RESTRICT = 219,
    REVOKE = 220,
    RIGHT = 221,
    RLIKE = 222,
    ROLE = 223,
    ROLES = 224,
    ROLLBACK = 225,
    ROLLUP = 226,
    ROW = 227,
    ROWS = 228,
    SECOND = 229,
    SECONDS = 230,
    SCHEMA = 231,
    SCHEMAS = 232,
    SELECT = 233,
    SEMI = 234,
    SEPARATED = 235,
    SERDE = 236,
    SERDEPROPERTIES = 237,
    SESSION_USER = 238,
    SET = 239,
    SETMINUS = 240,
    SETS = 241,
    SHOW = 242,
    SKEWED = 243,
    SOME = 244,
    SORT = 245,
    SORTED = 246,
    SOURCE = 247,
    START = 248,
    STATISTICS = 249,
    STORED = 250,
    STRATIFY = 251,
    STRUCT = 252,
    SUBSTR = 253,
    SUBSTRING = 254,
    SYNC = 255,
    SYSTEM_TIME = 256,
    SYSTEM_VERSION = 257,
    TABLE = 258,
    TABLES = 259,
    TABLESAMPLE = 260,
    TARGET = 261,
    TBLPROPERTIES = 262,
    TEMPORARY = 263,
    TERMINATED = 264,
    THEN = 265,
    TIME = 266,
    TIMESTAMP = 267,
    TIMESTAMPADD = 268,
    TIMESTAMPDIFF = 269,
    TO = 270,
    TOUCH = 271,
    TRAILING = 272,
    TRANSACTION = 273,
    TRANSACTIONS = 274,
    TRANSFORM = 275,
    TRIM = 276,
    TRUE = 277,
    TRUNCATE = 278,
    TRY_CAST = 279,
    TYPE = 280,
    UNARCHIVE = 281,
    UNBOUNDED = 282,
    UNCACHE = 283,
    UNION = 284,
    UNIQUE = 285,
    UNKNOWN = 286,
    UNLOCK = 287,
    UNPIVOT = 288,
    UNSET = 289,
    UPDATE = 290,
    USE = 291,
    USER = 292,
    USING = 293,
    VALUES = 294,
    VERSION = 295,
    VIEW = 296,
    VIEWS = 297,
    WEEK = 298,
    WEEKS = 299,
    WHEN = 300,
    WHERE = 301,
    WINDOW = 302,
    WITH = 303,
    WITHIN = 304,
    YEAR = 305,
    YEARS = 306,
    ZONE = 307,
    EQ = 308,
    NSEQ = 309,
    NEQ = 310,
    NEQJ = 311,
    LT = 312,
    LTE = 313,
    GT = 314,
    GTE = 315,
    PLUS = 316,
    MINUS = 317,
    ASTERISK = 318,
    SLASH = 319,
    PERCENT = 320,
    TILDE = 321,
    AMPERSAND = 322,
    PIPE = 323,
    CONCAT_PIPE = 324,
    HAT = 325,
    COLON = 326,
    ARROW = 327,
    HENT_START = 328,
    HENT_END = 329,
    STRING = 330,
    DOUBLEQUOTED_STRING = 331,
    BIGINT_LITERAL = 332,
    SMALLINT_LITERAL = 333,
    TINYINT_LITERAL = 334,
    INTEGER_VALUE = 335,
    EXPONENT_VALUE = 336,
    DECIMAL_VALUE = 337,
    FLOAT_LITERAL = 338,
    DOUBLE_LITERAL = 339,
    BIGDECIMAL_LITERAL = 340,
    IDENTIFIER = 341,
    BACKQUOTED_IDENTIFIER = 342,
    SIMPLE_COMMENT = 343,
    BRACKETED_COMMENT = 344,
    WS = 345,
    UNRECOGNIZED = 346
  };

  enum {
    RuleSingleStatement = 0,
    RuleSingleExpression = 1,
    RuleSingleTableIdentifier = 2,
    RuleSingleMultipartIdentifier = 3,
    RuleSingleFunctionIdentifier = 4,
    RuleSingleDataType = 5,
    RuleSingleTableSchema = 6,
    RuleStatement = 7,
    RuleTimezone = 8,
    RuleConfigKey = 9,
    RuleConfigValue = 10,
    RuleUnsupportedHiveNativeCommands = 11,
    RuleCreateTableHeader = 12,
    RuleReplaceTableHeader = 13,
    RuleBucketSpec = 14,
    RuleSkewSpec = 15,
    RuleLocationSpec = 16,
    RuleCommentSpec = 17,
    RuleQuery = 18,
    RuleInsertInto = 19,
    RulePartitionSpecLocation = 20,
    RulePartitionSpec = 21,
    RulePartitionVal = 22,
    RuleNamespaceKeyword = 23,
    RuleNamespaces = 24,
    RuleDescribeFuncName = 25,
    RuleDescribeColName = 26,
    RuleCtes = 27,
    RuleNamedQuery = 28,
    RuleTableProvider = 29,
    RuleCreateTableClauses = 30,
    RulePropertyList = 31,
    RuleProperty = 32,
    RulePropertyKey = 33,
    RulePropertyValue = 34,
    RuleConstantList = 35,
    RuleNestedConstantList = 36,
    RuleCreateFileFormat = 37,
    RuleFileFormat = 38,
    RuleStorageHandler = 39,
    RuleResource = 40,
    RuleDmlStatementNoWith = 41,
    RuleQueryOrganization = 42,
    RuleMultiInsertQueryBody = 43,
    RuleQueryTerm = 44,
    RuleQueryPrimary = 45,
    RuleSortItem = 46,
    RuleFromStatement = 47,
    RuleFromStatementBody = 48,
    RuleQuerySpecification = 49,
    RuleTransformClause = 50,
    RuleSelectClause = 51,
    RuleSetClause = 52,
    RuleMatchedClause = 53,
    RuleNotMatchedClause = 54,
    RuleNotMatchedBySourceClause = 55,
    RuleMatchedAction = 56,
    RuleNotMatchedAction = 57,
    RuleNotMatchedBySourceAction = 58,
    RuleAssignmentList = 59,
    RuleAssignment = 60,
    RuleWhereClause = 61,
    RuleHavingClause = 62,
    RuleHint = 63,
    RuleHintStatement = 64,
    RuleFromClause = 65,
    RuleTemporalClause = 66,
    RuleAggregationClause = 67,
    RuleGroupByClause = 68,
    RuleGroupingAnalytics = 69,
    RuleGroupingElement = 70,
    RuleGroupingSet = 71,
    RulePivotClause = 72,
    RulePivotColumn = 73,
    RulePivotValue = 74,
    RuleUnpivotClause = 75,
    RuleUnpivotNullClause = 76,
    RuleUnpivotOperator = 77,
    RuleUnpivotSingleValueColumnClause = 78,
    RuleUnpivotMultiValueColumnClause = 79,
    RuleUnpivotColumnSet = 80,
    RuleUnpivotValueColumn = 81,
    RuleUnpivotNameColumn = 82,
    RuleUnpivotColumnAndAlias = 83,
    RuleUnpivotColumn = 84,
    RuleUnpivotAlias = 85,
    RuleLateralView = 86,
    RuleSetQuantifier = 87,
    RuleRelation = 88,
    RuleRelationExtension = 89,
    RuleJoinRelation = 90,
    RuleJoinType = 91,
    RuleJoinCriteria = 92,
    RuleSample = 93,
    RuleSampleMethod = 94,
    RuleIdentifierList = 95,
    RuleIdentifierSeq = 96,
    RuleOrderedIdentifierList = 97,
    RuleOrderedIdentifier = 98,
    RuleIdentifierCommentList = 99,
    RuleIdentifierComment = 100,
    RuleRelationPrimary = 101,
    RuleInlineTable = 102,
    RuleFunctionTable = 103,
    RuleTableAlias = 104,
    RuleRowFormat = 105,
    RuleMultipartIdentifierList = 106,
    RuleMultipartIdentifier = 107,
    RuleMultipartIdentifierPropertyList = 108,
    RuleMultipartIdentifierProperty = 109,
    RuleTableIdentifier = 110,
    RuleFunctionIdentifier = 111,
    RuleNamedExpression = 112,
    RuleNamedExpressionSeq = 113,
    RulePartitionFieldList = 114,
    RulePartitionField = 115,
    RuleTransform = 116,
    RuleTransformArgument = 117,
    RuleExpression = 118,
    RuleExpressionSeq = 119,
    RuleBooleanExpression = 120,
    RulePredicate = 121,
    RuleValueExpression = 122,
    RuleDatetimeUnit = 123,
    RulePrimaryExpression = 124,
    RuleConstant = 125,
    RuleComparisonOperator = 126,
    RuleArithmeticOperator = 127,
    RulePredicateOperator = 128,
    RuleBooleanValue = 129,
    RuleInterval = 130,
    RuleErrorCapturingMultiUnitsInterval = 131,
    RuleMultiUnitsInterval = 132,
    RuleErrorCapturingUnitToUnitInterval = 133,
    RuleUnitToUnitInterval = 134,
    RuleIntervalValue = 135,
    RuleUnitInMultiUnits = 136,
    RuleUnitInUnitToUnit = 137,
    RuleColPosition = 138,
    RuleDataType = 139,
    RuleQualifiedColTypeWithPositionList = 140,
    RuleQualifiedColTypeWithPosition = 141,
    RuleDefaultExpression = 142,
    RuleColTypeList = 143,
    RuleColType = 144,
    RuleCreateOrReplaceTableColTypeList = 145,
    RuleCreateOrReplaceTableColType = 146,
    RuleColDefinitionOption = 147,
    RuleGenerationExpression = 148,
    RuleComplexColTypeList = 149,
    RuleComplexColType = 150,
    RuleWhenClause = 151,
    RuleWindowClause = 152,
    RuleNamedWindow = 153,
    RuleWindowSpec = 154,
    RuleWindowFrame = 155,
    RuleFrameBound = 156,
    RuleQualifiedNameList = 157,
    RuleFunctionName = 158,
    RuleQualifiedName = 159,
    RuleErrorCapturingIdentifier = 160,
    RuleErrorCapturingIdentifierExtra = 161,
    RuleIdentifier = 162,
    RuleStrictIdentifier = 163,
    RuleQuotedIdentifier = 164,
    RuleBackQuotedIdentifier = 165,
    RuleNumber = 166,
    RuleAlterColumnAction = 167,
    RuleStringLit = 168,
    RuleComment = 169,
    RuleVersion = 170,
    RuleAnsiNonReserved = 171,
    RuleStrictNonReserved = 172,
    RuleNonReserved = 173
  };

  explicit SqlBaseParser(antlr4::TokenStream* input);

  SqlBaseParser(
      antlr4::TokenStream* input,
      const antlr4::atn::ParserATNSimulatorOptions& options);

  ~SqlBaseParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  /**
   * When false, INTERSECT is given the greater precedence over the other set
   * operations (UNION, EXCEPT and MINUS) as per the SQL standard.
   */
  bool legacy_setops_precedence_enabled = false;

  /**
   * When false, a literal with an exponent would be converted into
   * double type rather than decimal type.
   */
  bool legacy_exponent_literal_as_decimal_enabled = false;

  /**
   * When true, the behavior of keywords follows ANSI SQL standard.
   */
  bool SQL_standard_keyword_behavior = false;

  /**
   * When true, double quoted literals are identifiers rather than STRINGs.
   */
  bool double_quoted_identifiers = false;

  class SingleStatementContext;
  class SingleExpressionContext;
  class SingleTableIdentifierContext;
  class SingleMultipartIdentifierContext;
  class SingleFunctionIdentifierContext;
  class SingleDataTypeContext;
  class SingleTableSchemaContext;
  class StatementContext;
  class TimezoneContext;
  class ConfigKeyContext;
  class ConfigValueContext;
  class UnsupportedHiveNativeCommandsContext;
  class CreateTableHeaderContext;
  class ReplaceTableHeaderContext;
  class BucketSpecContext;
  class SkewSpecContext;
  class LocationSpecContext;
  class CommentSpecContext;
  class QueryContext;
  class InsertIntoContext;
  class PartitionSpecLocationContext;
  class PartitionSpecContext;
  class PartitionValContext;
  class NamespaceKeywordContext;
  class NamespacesContext;
  class DescribeFuncNameContext;
  class DescribeColNameContext;
  class CtesContext;
  class NamedQueryContext;
  class TableProviderContext;
  class CreateTableClausesContext;
  class PropertyListContext;
  class PropertyContext;
  class PropertyKeyContext;
  class PropertyValueContext;
  class ConstantListContext;
  class NestedConstantListContext;
  class CreateFileFormatContext;
  class FileFormatContext;
  class StorageHandlerContext;
  class ResourceContext;
  class DmlStatementNoWithContext;
  class QueryOrganizationContext;
  class MultiInsertQueryBodyContext;
  class QueryTermContext;
  class QueryPrimaryContext;
  class SortItemContext;
  class FromStatementContext;
  class FromStatementBodyContext;
  class QuerySpecificationContext;
  class TransformClauseContext;
  class SelectClauseContext;
  class SetClauseContext;
  class MatchedClauseContext;
  class NotMatchedClauseContext;
  class NotMatchedBySourceClauseContext;
  class MatchedActionContext;
  class NotMatchedActionContext;
  class NotMatchedBySourceActionContext;
  class AssignmentListContext;
  class AssignmentContext;
  class WhereClauseContext;
  class HavingClauseContext;
  class HintContext;
  class HintStatementContext;
  class FromClauseContext;
  class TemporalClauseContext;
  class AggregationClauseContext;
  class GroupByClauseContext;
  class GroupingAnalyticsContext;
  class GroupingElementContext;
  class GroupingSetContext;
  class PivotClauseContext;
  class PivotColumnContext;
  class PivotValueContext;
  class UnpivotClauseContext;
  class UnpivotNullClauseContext;
  class UnpivotOperatorContext;
  class UnpivotSingleValueColumnClauseContext;
  class UnpivotMultiValueColumnClauseContext;
  class UnpivotColumnSetContext;
  class UnpivotValueColumnContext;
  class UnpivotNameColumnContext;
  class UnpivotColumnAndAliasContext;
  class UnpivotColumnContext;
  class UnpivotAliasContext;
  class LateralViewContext;
  class SetQuantifierContext;
  class RelationContext;
  class RelationExtensionContext;
  class JoinRelationContext;
  class JoinTypeContext;
  class JoinCriteriaContext;
  class SampleContext;
  class SampleMethodContext;
  class IdentifierListContext;
  class IdentifierSeqContext;
  class OrderedIdentifierListContext;
  class OrderedIdentifierContext;
  class IdentifierCommentListContext;
  class IdentifierCommentContext;
  class RelationPrimaryContext;
  class InlineTableContext;
  class FunctionTableContext;
  class TableAliasContext;
  class RowFormatContext;
  class MultipartIdentifierListContext;
  class MultipartIdentifierContext;
  class MultipartIdentifierPropertyListContext;
  class MultipartIdentifierPropertyContext;
  class TableIdentifierContext;
  class FunctionIdentifierContext;
  class NamedExpressionContext;
  class NamedExpressionSeqContext;
  class PartitionFieldListContext;
  class PartitionFieldContext;
  class TransformContext;
  class TransformArgumentContext;
  class ExpressionContext;
  class ExpressionSeqContext;
  class BooleanExpressionContext;
  class PredicateContext;
  class ValueExpressionContext;
  class DatetimeUnitContext;
  class PrimaryExpressionContext;
  class ConstantContext;
  class ComparisonOperatorContext;
  class ArithmeticOperatorContext;
  class PredicateOperatorContext;
  class BooleanValueContext;
  class IntervalContext;
  class ErrorCapturingMultiUnitsIntervalContext;
  class MultiUnitsIntervalContext;
  class ErrorCapturingUnitToUnitIntervalContext;
  class UnitToUnitIntervalContext;
  class IntervalValueContext;
  class UnitInMultiUnitsContext;
  class UnitInUnitToUnitContext;
  class ColPositionContext;
  class DataTypeContext;
  class QualifiedColTypeWithPositionListContext;
  class QualifiedColTypeWithPositionContext;
  class DefaultExpressionContext;
  class ColTypeListContext;
  class ColTypeContext;
  class CreateOrReplaceTableColTypeListContext;
  class CreateOrReplaceTableColTypeContext;
  class ColDefinitionOptionContext;
  class GenerationExpressionContext;
  class ComplexColTypeListContext;
  class ComplexColTypeContext;
  class WhenClauseContext;
  class WindowClauseContext;
  class NamedWindowContext;
  class WindowSpecContext;
  class WindowFrameContext;
  class FrameBoundContext;
  class QualifiedNameListContext;
  class FunctionNameContext;
  class QualifiedNameContext;
  class ErrorCapturingIdentifierContext;
  class ErrorCapturingIdentifierExtraContext;
  class IdentifierContext;
  class StrictIdentifierContext;
  class QuotedIdentifierContext;
  class BackQuotedIdentifierContext;
  class NumberContext;
  class AlterColumnActionContext;
  class StringLitContext;
  class CommentContext;
  class VersionContext;
  class AnsiNonReservedContext;
  class StrictNonReservedContext;
  class NonReservedContext;

  class SingleStatementContext : public antlr4::ParserRuleContext {
   public:
    SingleStatementContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StatementContext* statement();
    antlr4::tree::TerminalNode* EOF();
    std::vector<antlr4::tree::TerminalNode*> SEMICOLON();
    antlr4::tree::TerminalNode* SEMICOLON(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SingleStatementContext* singleStatement();

  class SingleExpressionContext : public antlr4::ParserRuleContext {
   public:
    SingleExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NamedExpressionContext* namedExpression();
    antlr4::tree::TerminalNode* EOF();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SingleExpressionContext* singleExpression();

  class SingleTableIdentifierContext : public antlr4::ParserRuleContext {
   public:
    SingleTableIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TableIdentifierContext* tableIdentifier();
    antlr4::tree::TerminalNode* EOF();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SingleTableIdentifierContext* singleTableIdentifier();

  class SingleMultipartIdentifierContext : public antlr4::ParserRuleContext {
   public:
    SingleMultipartIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* EOF();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SingleMultipartIdentifierContext* singleMultipartIdentifier();

  class SingleFunctionIdentifierContext : public antlr4::ParserRuleContext {
   public:
    SingleFunctionIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    FunctionIdentifierContext* functionIdentifier();
    antlr4::tree::TerminalNode* EOF();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SingleFunctionIdentifierContext* singleFunctionIdentifier();

  class SingleDataTypeContext : public antlr4::ParserRuleContext {
   public:
    SingleDataTypeContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DataTypeContext* dataType();
    antlr4::tree::TerminalNode* EOF();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SingleDataTypeContext* singleDataType();

  class SingleTableSchemaContext : public antlr4::ParserRuleContext {
   public:
    SingleTableSchemaContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ColTypeListContext* colTypeList();
    antlr4::tree::TerminalNode* EOF();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SingleTableSchemaContext* singleTableSchema();

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
    antlr4::tree::TerminalNode* LOGICAL();
    antlr4::tree::TerminalNode* FORMATTED();
    antlr4::tree::TerminalNode* EXTENDED();
    antlr4::tree::TerminalNode* CODEGEN();
    antlr4::tree::TerminalNode* COST();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ResetConfigurationContext : public StatementContext {
   public:
    ResetConfigurationContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* RESET();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AlterViewQueryContext : public StatementContext {
   public:
    AlterViewQueryContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* VIEW();
    MultipartIdentifierContext* multipartIdentifier();
    QueryContext* query();
    antlr4::tree::TerminalNode* AS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UseContext : public StatementContext {
   public:
    UseContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* USE();
    MultipartIdentifierContext* multipartIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropNamespaceContext : public StatementContext {
   public:
    DropNamespaceContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DROP();
    NamespaceKeywordContext* namespaceKeyword();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* RESTRICT();
    antlr4::tree::TerminalNode* CASCADE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateTempViewUsingContext : public StatementContext {
   public:
    CreateTempViewUsingContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* TEMPORARY();
    antlr4::tree::TerminalNode* VIEW();
    TableIdentifierContext* tableIdentifier();
    TableProviderContext* tableProvider();
    antlr4::tree::TerminalNode* OR();
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* GLOBAL();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    ColTypeListContext* colTypeList();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* OPTIONS();
    PropertyListContext* propertyList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RenameTableContext : public StatementContext {
   public:
    RenameTableContext(StatementContext* ctx);

    SqlBaseParser::MultipartIdentifierContext* from = nullptr;
    SqlBaseParser::MultipartIdentifierContext* to = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* RENAME();
    antlr4::tree::TerminalNode* TO();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* VIEW();
    std::vector<MultipartIdentifierContext*> multipartIdentifier();
    MultipartIdentifierContext* multipartIdentifier(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class FailNativeCommandContext : public StatementContext {
   public:
    FailNativeCommandContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* ROLE();
    UnsupportedHiveNativeCommandsContext* unsupportedHiveNativeCommands();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetCatalogContext : public StatementContext {
   public:
    SetCatalogContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* CATALOG();
    IdentifierContext* identifier();
    StringLitContext* stringLit();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ClearCacheContext : public StatementContext {
   public:
    ClearCacheContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CLEAR();
    antlr4::tree::TerminalNode* CACHE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropViewContext : public StatementContext {
   public:
    DropViewContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* VIEW();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowTablesContext : public StatementContext {
   public:
    ShowTablesContext(StatementContext* ctx);

    SqlBaseParser::StringLitContext* pattern = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* TABLES();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* LIKE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RecoverPartitionsContext : public StatementContext {
   public:
    RecoverPartitionsContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* RECOVER();
    antlr4::tree::TerminalNode* PARTITIONS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropIndexContext : public StatementContext {
   public:
    DropIndexContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* INDEX();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* ON();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* TABLE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowCatalogsContext : public StatementContext {
   public:
    ShowCatalogsContext(StatementContext* ctx);

    SqlBaseParser::StringLitContext* pattern = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* CATALOGS();
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* LIKE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowCurrentNamespaceContext : public StatementContext {
   public:
    ShowCurrentNamespaceContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* CURRENT();
    NamespaceKeywordContext* namespaceKeyword();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RenameTablePartitionContext : public StatementContext {
   public:
    RenameTablePartitionContext(StatementContext* ctx);

    SqlBaseParser::PartitionSpecContext* from = nullptr;
    SqlBaseParser::PartitionSpecContext* to = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* RENAME();
    antlr4::tree::TerminalNode* TO();
    std::vector<PartitionSpecContext*> partitionSpec();
    PartitionSpecContext* partitionSpec(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RepairTableContext : public StatementContext {
   public:
    RepairTableContext(StatementContext* ctx);

    antlr4::Token* option = nullptr;
    antlr4::tree::TerminalNode* REPAIR();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* MSCK();
    antlr4::tree::TerminalNode* PARTITIONS();
    antlr4::tree::TerminalNode* ADD();
    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* SYNC();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RefreshResourceContext : public StatementContext {
   public:
    RefreshResourceContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* REFRESH();
    StringLitContext* stringLit();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowCreateTableContext : public StatementContext {
   public:
    ShowCreateTableContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* AS();
    antlr4::tree::TerminalNode* SERDE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowNamespacesContext : public StatementContext {
   public:
    ShowNamespacesContext(StatementContext* ctx);

    SqlBaseParser::StringLitContext* pattern = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    NamespacesContext* namespaces();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* LIKE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowColumnsContext : public StatementContext {
   public:
    ShowColumnsContext(StatementContext* ctx);

    SqlBaseParser::MultipartIdentifierContext* table = nullptr;
    SqlBaseParser::MultipartIdentifierContext* ns = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* COLUMNS();
    std::vector<antlr4::tree::TerminalNode*> FROM();
    antlr4::tree::TerminalNode* FROM(size_t i);
    std::vector<antlr4::tree::TerminalNode*> IN();
    antlr4::tree::TerminalNode* IN(size_t i);
    std::vector<MultipartIdentifierContext*> multipartIdentifier();
    MultipartIdentifierContext* multipartIdentifier(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ReplaceTableContext : public StatementContext {
   public:
    ReplaceTableContext(StatementContext* ctx);

    ReplaceTableHeaderContext* replaceTableHeader();
    CreateTableClausesContext* createTableClauses();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    CreateOrReplaceTableColTypeListContext* createOrReplaceTableColTypeList();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    TableProviderContext* tableProvider();
    QueryContext* query();
    antlr4::tree::TerminalNode* AS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AnalyzeTablesContext : public StatementContext {
   public:
    AnalyzeTablesContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ANALYZE();
    antlr4::tree::TerminalNode* TABLES();
    antlr4::tree::TerminalNode* COMPUTE();
    antlr4::tree::TerminalNode* STATISTICS();
    MultipartIdentifierContext* multipartIdentifier();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AddTablePartitionContext : public StatementContext {
   public:
    AddTablePartitionContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* ADD();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* VIEW();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    std::vector<PartitionSpecLocationContext*> partitionSpecLocation();
    PartitionSpecLocationContext* partitionSpecLocation(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetNamespaceLocationContext : public StatementContext {
   public:
    SetNamespaceLocationContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    NamespaceKeywordContext* namespaceKeyword();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* SET();
    LocationSpecContext* locationSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RefreshTableContext : public StatementContext {
   public:
    RefreshTableContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* REFRESH();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetNamespacePropertiesContext : public StatementContext {
   public:
    SetNamespacePropertiesContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    NamespaceKeywordContext* namespaceKeyword();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* SET();
    PropertyListContext* propertyList();
    antlr4::tree::TerminalNode* DBPROPERTIES();
    antlr4::tree::TerminalNode* PROPERTIES();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ManageResourceContext : public StatementContext {
   public:
    ManageResourceContext(StatementContext* ctx);

    antlr4::Token* op = nullptr;
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* ADD();
    antlr4::tree::TerminalNode* LIST();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetQuotedConfigurationContext : public StatementContext {
   public:
    SetQuotedConfigurationContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SET();
    ConfigKeyContext* configKey();
    antlr4::tree::TerminalNode* EQ();
    ConfigValueContext* configValue();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AnalyzeContext : public StatementContext {
   public:
    AnalyzeContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ANALYZE();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* COMPUTE();
    antlr4::tree::TerminalNode* STATISTICS();
    PartitionSpecContext* partitionSpec();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* FOR();
    antlr4::tree::TerminalNode* COLUMNS();
    IdentifierSeqContext* identifierSeq();
    antlr4::tree::TerminalNode* ALL();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateFunctionContext : public StatementContext {
   public:
    CreateFunctionContext(StatementContext* ctx);

    SqlBaseParser::StringLitContext* className = nullptr;
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* FUNCTION();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* AS();
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* OR();
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* TEMPORARY();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* USING();
    std::vector<ResourceContext*> resource();
    ResourceContext* resource(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class HiveReplaceColumnsContext : public StatementContext {
   public:
    HiveReplaceColumnsContext(StatementContext* ctx);

    SqlBaseParser::MultipartIdentifierContext* table = nullptr;
    SqlBaseParser::QualifiedColTypeWithPositionListContext* columns = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* COLUMNS();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    MultipartIdentifierContext* multipartIdentifier();
    QualifiedColTypeWithPositionListContext* qualifiedColTypeWithPositionList();
    PartitionSpecContext* partitionSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CommentNamespaceContext : public StatementContext {
   public:
    CommentNamespaceContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* COMMENT();
    antlr4::tree::TerminalNode* ON();
    NamespaceKeywordContext* namespaceKeyword();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* IS();
    CommentContext* comment();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ResetQuotedConfigurationContext : public StatementContext {
   public:
    ResetQuotedConfigurationContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* RESET();
    ConfigKeyContext* configKey();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateTableContext : public StatementContext {
   public:
    CreateTableContext(StatementContext* ctx);

    CreateTableHeaderContext* createTableHeader();
    CreateTableClausesContext* createTableClauses();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    CreateOrReplaceTableColTypeListContext* createOrReplaceTableColTypeList();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    TableProviderContext* tableProvider();
    QueryContext* query();
    antlr4::tree::TerminalNode* AS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DmlStatementContext : public StatementContext {
   public:
    DmlStatementContext(StatementContext* ctx);

    DmlStatementNoWithContext* dmlStatementNoWith();
    CtesContext* ctes();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateTableLikeContext : public StatementContext {
   public:
    CreateTableLikeContext(StatementContext* ctx);

    SqlBaseParser::TableIdentifierContext* target = nullptr;
    SqlBaseParser::TableIdentifierContext* source = nullptr;
    SqlBaseParser::PropertyListContext* tableProps = nullptr;
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* LIKE();
    std::vector<TableIdentifierContext*> tableIdentifier();
    TableIdentifierContext* tableIdentifier(size_t i);
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    std::vector<TableProviderContext*> tableProvider();
    TableProviderContext* tableProvider(size_t i);
    std::vector<RowFormatContext*> rowFormat();
    RowFormatContext* rowFormat(size_t i);
    std::vector<CreateFileFormatContext*> createFileFormat();
    CreateFileFormatContext* createFileFormat(size_t i);
    std::vector<LocationSpecContext*> locationSpec();
    LocationSpecContext* locationSpec(size_t i);
    std::vector<antlr4::tree::TerminalNode*> TBLPROPERTIES();
    antlr4::tree::TerminalNode* TBLPROPERTIES(size_t i);
    std::vector<PropertyListContext*> propertyList();
    PropertyListContext* propertyList(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UncacheTableContext : public StatementContext {
   public:
    UncacheTableContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* UNCACHE();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropFunctionContext : public StatementContext {
   public:
    DropFunctionContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* FUNCTION();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* TEMPORARY();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DescribeRelationContext : public StatementContext {
   public:
    DescribeRelationContext(StatementContext* ctx);

    antlr4::Token* option = nullptr;
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* DESC();
    antlr4::tree::TerminalNode* DESCRIBE();
    antlr4::tree::TerminalNode* TABLE();
    PartitionSpecContext* partitionSpec();
    DescribeColNameContext* describeColName();
    antlr4::tree::TerminalNode* EXTENDED();
    antlr4::tree::TerminalNode* FORMATTED();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LoadDataContext : public StatementContext {
   public:
    LoadDataContext(StatementContext* ctx);

    SqlBaseParser::StringLitContext* path = nullptr;
    antlr4::tree::TerminalNode* LOAD();
    antlr4::tree::TerminalNode* DATA();
    antlr4::tree::TerminalNode* INPATH();
    antlr4::tree::TerminalNode* INTO();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* LOCAL();
    antlr4::tree::TerminalNode* OVERWRITE();
    PartitionSpecContext* partitionSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowPartitionsContext : public StatementContext {
   public:
    ShowPartitionsContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* PARTITIONS();
    MultipartIdentifierContext* multipartIdentifier();
    PartitionSpecContext* partitionSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DescribeFunctionContext : public StatementContext {
   public:
    DescribeFunctionContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* FUNCTION();
    DescribeFuncNameContext* describeFuncName();
    antlr4::tree::TerminalNode* DESC();
    antlr4::tree::TerminalNode* DESCRIBE();
    antlr4::tree::TerminalNode* EXTENDED();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RenameTableColumnContext : public StatementContext {
   public:
    RenameTableColumnContext(StatementContext* ctx);

    SqlBaseParser::MultipartIdentifierContext* table = nullptr;
    SqlBaseParser::MultipartIdentifierContext* from = nullptr;
    SqlBaseParser::ErrorCapturingIdentifierContext* to = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* RENAME();
    antlr4::tree::TerminalNode* COLUMN();
    antlr4::tree::TerminalNode* TO();
    std::vector<MultipartIdentifierContext*> multipartIdentifier();
    MultipartIdentifierContext* multipartIdentifier(size_t i);
    ErrorCapturingIdentifierContext* errorCapturingIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class StatementDefaultContext : public StatementContext {
   public:
    StatementDefaultContext(StatementContext* ctx);

    QueryContext* query();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class HiveChangeColumnContext : public StatementContext {
   public:
    HiveChangeColumnContext(StatementContext* ctx);

    SqlBaseParser::MultipartIdentifierContext* table = nullptr;
    SqlBaseParser::MultipartIdentifierContext* colName = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* CHANGE();
    ColTypeContext* colType();
    std::vector<MultipartIdentifierContext*> multipartIdentifier();
    MultipartIdentifierContext* multipartIdentifier(size_t i);
    PartitionSpecContext* partitionSpec();
    antlr4::tree::TerminalNode* COLUMN();
    ColPositionContext* colPosition();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetTimeZoneContext : public StatementContext {
   public:
    SetTimeZoneContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* TIME();
    antlr4::tree::TerminalNode* ZONE();
    IntervalContext* interval();
    TimezoneContext* timezone();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DescribeQueryContext : public StatementContext {
   public:
    DescribeQueryContext(StatementContext* ctx);

    QueryContext* query();
    antlr4::tree::TerminalNode* DESC();
    antlr4::tree::TerminalNode* DESCRIBE();
    antlr4::tree::TerminalNode* QUERY();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TruncateTableContext : public StatementContext {
   public:
    TruncateTableContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* TRUNCATE();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    PartitionSpecContext* partitionSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetTableSerDeContext : public StatementContext {
   public:
    SetTableSerDeContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* SERDE();
    StringLitContext* stringLit();
    PartitionSpecContext* partitionSpec();
    antlr4::tree::TerminalNode* WITH();
    antlr4::tree::TerminalNode* SERDEPROPERTIES();
    PropertyListContext* propertyList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateViewContext : public StatementContext {
   public:
    CreateViewContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* VIEW();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* AS();
    QueryContext* query();
    antlr4::tree::TerminalNode* OR();
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* TEMPORARY();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    IdentifierCommentListContext* identifierCommentList();
    std::vector<CommentSpecContext*> commentSpec();
    CommentSpecContext* commentSpec(size_t i);
    std::vector<antlr4::tree::TerminalNode*> PARTITIONED();
    antlr4::tree::TerminalNode* PARTITIONED(size_t i);
    std::vector<antlr4::tree::TerminalNode*> ON();
    antlr4::tree::TerminalNode* ON(size_t i);
    std::vector<IdentifierListContext*> identifierList();
    IdentifierListContext* identifierList(size_t i);
    std::vector<antlr4::tree::TerminalNode*> TBLPROPERTIES();
    antlr4::tree::TerminalNode* TBLPROPERTIES(size_t i);
    std::vector<PropertyListContext*> propertyList();
    PropertyListContext* propertyList(size_t i);
    antlr4::tree::TerminalNode* GLOBAL();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropTablePartitionsContext : public StatementContext {
   public:
    DropTablePartitionsContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* DROP();
    std::vector<PartitionSpecContext*> partitionSpec();
    PartitionSpecContext* partitionSpec(size_t i);
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* VIEW();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode* PURGE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetConfigurationContext : public StatementContext {
   public:
    SetConfigurationContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* SET();
    ConfigKeyContext* configKey();
    antlr4::tree::TerminalNode* EQ();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropTableContext : public StatementContext {
   public:
    DropTableContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* PURGE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowTableExtendedContext : public StatementContext {
   public:
    ShowTableExtendedContext(StatementContext* ctx);

    SqlBaseParser::MultipartIdentifierContext* ns = nullptr;
    SqlBaseParser::StringLitContext* pattern = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* EXTENDED();
    antlr4::tree::TerminalNode* LIKE();
    StringLitContext* stringLit();
    PartitionSpecContext* partitionSpec();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();
    MultipartIdentifierContext* multipartIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DescribeNamespaceContext : public StatementContext {
   public:
    DescribeNamespaceContext(StatementContext* ctx);

    NamespaceKeywordContext* namespaceKeyword();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* DESC();
    antlr4::tree::TerminalNode* DESCRIBE();
    antlr4::tree::TerminalNode* EXTENDED();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AlterTableAlterColumnContext : public StatementContext {
   public:
    AlterTableAlterColumnContext(StatementContext* ctx);

    SqlBaseParser::MultipartIdentifierContext* table = nullptr;
    SqlBaseParser::MultipartIdentifierContext* column = nullptr;
    std::vector<antlr4::tree::TerminalNode*> ALTER();
    antlr4::tree::TerminalNode* ALTER(size_t i);
    antlr4::tree::TerminalNode* TABLE();
    std::vector<MultipartIdentifierContext*> multipartIdentifier();
    MultipartIdentifierContext* multipartIdentifier(size_t i);
    antlr4::tree::TerminalNode* CHANGE();
    antlr4::tree::TerminalNode* COLUMN();
    AlterColumnActionContext* alterColumnAction();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RefreshFunctionContext : public StatementContext {
   public:
    RefreshFunctionContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* REFRESH();
    antlr4::tree::TerminalNode* FUNCTION();
    MultipartIdentifierContext* multipartIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CommentTableContext : public StatementContext {
   public:
    CommentTableContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* COMMENT();
    antlr4::tree::TerminalNode* ON();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* IS();
    CommentContext* comment();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateIndexContext : public StatementContext {
   public:
    CreateIndexContext(StatementContext* ctx);

    SqlBaseParser::IdentifierContext* indexType = nullptr;
    SqlBaseParser::MultipartIdentifierPropertyListContext* columns = nullptr;
    SqlBaseParser::PropertyListContext* options = nullptr;
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* INDEX();
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    antlr4::tree::TerminalNode* ON();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    MultipartIdentifierPropertyListContext* multipartIdentifierPropertyList();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* USING();
    antlr4::tree::TerminalNode* OPTIONS();
    PropertyListContext* propertyList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UseNamespaceContext : public StatementContext {
   public:
    UseNamespaceContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* USE();
    NamespaceKeywordContext* namespaceKeyword();
    MultipartIdentifierContext* multipartIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CreateNamespaceContext : public StatementContext {
   public:
    CreateNamespaceContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* CREATE();
    NamespaceKeywordContext* namespaceKeyword();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    std::vector<CommentSpecContext*> commentSpec();
    CommentSpecContext* commentSpec(size_t i);
    std::vector<LocationSpecContext*> locationSpec();
    LocationSpecContext* locationSpec(size_t i);
    std::vector<antlr4::tree::TerminalNode*> WITH();
    antlr4::tree::TerminalNode* WITH(size_t i);
    std::vector<PropertyListContext*> propertyList();
    PropertyListContext* propertyList(size_t i);
    std::vector<antlr4::tree::TerminalNode*> DBPROPERTIES();
    antlr4::tree::TerminalNode* DBPROPERTIES(size_t i);
    std::vector<antlr4::tree::TerminalNode*> PROPERTIES();
    antlr4::tree::TerminalNode* PROPERTIES(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowTblPropertiesContext : public StatementContext {
   public:
    ShowTblPropertiesContext(StatementContext* ctx);

    SqlBaseParser::MultipartIdentifierContext* table = nullptr;
    SqlBaseParser::PropertyKeyContext* key = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* TBLPROPERTIES();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    PropertyKeyContext* propertyKey();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UnsetTablePropertiesContext : public StatementContext {
   public:
    UnsetTablePropertiesContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* UNSET();
    antlr4::tree::TerminalNode* TBLPROPERTIES();
    PropertyListContext* propertyList();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* VIEW();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetTableLocationContext : public StatementContext {
   public:
    SetTableLocationContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* SET();
    LocationSpecContext* locationSpec();
    PartitionSpecContext* partitionSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DropTableColumnsContext : public StatementContext {
   public:
    DropTableColumnsContext(StatementContext* ctx);

    SqlBaseParser::MultipartIdentifierListContext* columns = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* COLUMN();
    antlr4::tree::TerminalNode* COLUMNS();
    MultipartIdentifierListContext* multipartIdentifierList();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* EXISTS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowViewsContext : public StatementContext {
   public:
    ShowViewsContext(StatementContext* ctx);

    SqlBaseParser::StringLitContext* pattern = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* VIEWS();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* LIKE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ShowFunctionsContext : public StatementContext {
   public:
    ShowFunctionsContext(StatementContext* ctx);

    SqlBaseParser::MultipartIdentifierContext* ns = nullptr;
    SqlBaseParser::MultipartIdentifierContext* legacy = nullptr;
    SqlBaseParser::StringLitContext* pattern = nullptr;
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* FUNCTIONS();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* IN();
    std::vector<MultipartIdentifierContext*> multipartIdentifier();
    MultipartIdentifierContext* multipartIdentifier(size_t i);
    antlr4::tree::TerminalNode* LIKE();
    StringLitContext* stringLit();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CacheTableContext : public StatementContext {
   public:
    CacheTableContext(StatementContext* ctx);

    SqlBaseParser::PropertyListContext* options = nullptr;
    antlr4::tree::TerminalNode* CACHE();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* LAZY();
    antlr4::tree::TerminalNode* OPTIONS();
    QueryContext* query();
    PropertyListContext* propertyList();
    antlr4::tree::TerminalNode* AS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AddTableColumnsContext : public StatementContext {
   public:
    AddTableColumnsContext(StatementContext* ctx);

    SqlBaseParser::QualifiedColTypeWithPositionListContext* columns = nullptr;
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* ADD();
    antlr4::tree::TerminalNode* COLUMN();
    antlr4::tree::TerminalNode* COLUMNS();
    QualifiedColTypeWithPositionListContext* qualifiedColTypeWithPositionList();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetTablePropertiesContext : public StatementContext {
   public:
    SetTablePropertiesContext(StatementContext* ctx);

    antlr4::tree::TerminalNode* ALTER();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* TBLPROPERTIES();
    PropertyListContext* propertyList();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* VIEW();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StatementContext* statement();

  class TimezoneContext : public antlr4::ParserRuleContext {
   public:
    TimezoneContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* LOCAL();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TimezoneContext* timezone();

  class ConfigKeyContext : public antlr4::ParserRuleContext {
   public:
    ConfigKeyContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QuotedIdentifierContext* quotedIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConfigKeyContext* configKey();

  class ConfigValueContext : public antlr4::ParserRuleContext {
   public:
    ConfigValueContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BackQuotedIdentifierContext* backQuotedIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConfigValueContext* configValue();

  class UnsupportedHiveNativeCommandsContext
      : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* kw1 = nullptr;
    antlr4::Token* kw2 = nullptr;
    antlr4::Token* kw3 = nullptr;
    antlr4::Token* kw4 = nullptr;
    antlr4::Token* kw5 = nullptr;
    antlr4::Token* kw6 = nullptr;
    UnsupportedHiveNativeCommandsContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* ROLE();
    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* GRANT();
    antlr4::tree::TerminalNode* REVOKE();
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* PRINCIPALS();
    antlr4::tree::TerminalNode* ROLES();
    antlr4::tree::TerminalNode* CURRENT();
    antlr4::tree::TerminalNode* EXPORT();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* IMPORT();
    antlr4::tree::TerminalNode* COMPACTIONS();
    antlr4::tree::TerminalNode* TRANSACTIONS();
    antlr4::tree::TerminalNode* INDEXES();
    antlr4::tree::TerminalNode* LOCKS();
    antlr4::tree::TerminalNode* INDEX();
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* LOCK();
    antlr4::tree::TerminalNode* DATABASE();
    antlr4::tree::TerminalNode* UNLOCK();
    antlr4::tree::TerminalNode* TEMPORARY();
    antlr4::tree::TerminalNode* MACRO();
    TableIdentifierContext* tableIdentifier();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* CLUSTERED();
    antlr4::tree::TerminalNode* BY();
    antlr4::tree::TerminalNode* SORTED();
    antlr4::tree::TerminalNode* SKEWED();
    antlr4::tree::TerminalNode* STORED();
    antlr4::tree::TerminalNode* AS();
    antlr4::tree::TerminalNode* DIRECTORIES();
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* LOCATION();
    antlr4::tree::TerminalNode* EXCHANGE();
    antlr4::tree::TerminalNode* PARTITION();
    antlr4::tree::TerminalNode* ARCHIVE();
    antlr4::tree::TerminalNode* UNARCHIVE();
    antlr4::tree::TerminalNode* TOUCH();
    antlr4::tree::TerminalNode* COMPACT();
    PartitionSpecContext* partitionSpec();
    antlr4::tree::TerminalNode* CONCATENATE();
    antlr4::tree::TerminalNode* FILEFORMAT();
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* COLUMNS();
    antlr4::tree::TerminalNode* START();
    antlr4::tree::TerminalNode* TRANSACTION();
    antlr4::tree::TerminalNode* COMMIT();
    antlr4::tree::TerminalNode* ROLLBACK();
    antlr4::tree::TerminalNode* DFS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnsupportedHiveNativeCommandsContext* unsupportedHiveNativeCommands();

  class CreateTableHeaderContext : public antlr4::ParserRuleContext {
   public:
    CreateTableHeaderContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* TEMPORARY();
    antlr4::tree::TerminalNode* EXTERNAL();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CreateTableHeaderContext* createTableHeader();

  class ReplaceTableHeaderContext : public antlr4::ParserRuleContext {
   public:
    ReplaceTableHeaderContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* OR();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ReplaceTableHeaderContext* replaceTableHeader();

  class BucketSpecContext : public antlr4::ParserRuleContext {
   public:
    BucketSpecContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* CLUSTERED();
    std::vector<antlr4::tree::TerminalNode*> BY();
    antlr4::tree::TerminalNode* BY(size_t i);
    IdentifierListContext* identifierList();
    antlr4::tree::TerminalNode* INTO();
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    antlr4::tree::TerminalNode* BUCKETS();
    antlr4::tree::TerminalNode* SORTED();
    OrderedIdentifierListContext* orderedIdentifierList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BucketSpecContext* bucketSpec();

  class SkewSpecContext : public antlr4::ParserRuleContext {
   public:
    SkewSpecContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SKEWED();
    antlr4::tree::TerminalNode* BY();
    IdentifierListContext* identifierList();
    antlr4::tree::TerminalNode* ON();
    ConstantListContext* constantList();
    NestedConstantListContext* nestedConstantList();
    antlr4::tree::TerminalNode* STORED();
    antlr4::tree::TerminalNode* AS();
    antlr4::tree::TerminalNode* DIRECTORIES();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SkewSpecContext* skewSpec();

  class LocationSpecContext : public antlr4::ParserRuleContext {
   public:
    LocationSpecContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LOCATION();
    StringLitContext* stringLit();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LocationSpecContext* locationSpec();

  class CommentSpecContext : public antlr4::ParserRuleContext {
   public:
    CommentSpecContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* COMMENT();
    StringLitContext* stringLit();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CommentSpecContext* commentSpec();

  class QueryContext : public antlr4::ParserRuleContext {
   public:
    QueryContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryTermContext* queryTerm();
    QueryOrganizationContext* queryOrganization();
    CtesContext* ctes();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QueryContext* query();

  class InsertIntoContext : public antlr4::ParserRuleContext {
   public:
    InsertIntoContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    InsertIntoContext() = default;
    void copyFrom(InsertIntoContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class InsertIntoReplaceWhereContext : public InsertIntoContext {
   public:
    InsertIntoReplaceWhereContext(InsertIntoContext* ctx);

    antlr4::tree::TerminalNode* INSERT();
    antlr4::tree::TerminalNode* INTO();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* REPLACE();
    WhereClauseContext* whereClause();
    antlr4::tree::TerminalNode* TABLE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class InsertOverwriteHiveDirContext : public InsertIntoContext {
   public:
    InsertOverwriteHiveDirContext(InsertIntoContext* ctx);

    SqlBaseParser::StringLitContext* path = nullptr;
    antlr4::tree::TerminalNode* INSERT();
    antlr4::tree::TerminalNode* OVERWRITE();
    antlr4::tree::TerminalNode* DIRECTORY();
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* LOCAL();
    RowFormatContext* rowFormat();
    CreateFileFormatContext* createFileFormat();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class InsertOverwriteDirContext : public InsertIntoContext {
   public:
    InsertOverwriteDirContext(InsertIntoContext* ctx);

    SqlBaseParser::StringLitContext* path = nullptr;
    SqlBaseParser::PropertyListContext* options = nullptr;
    antlr4::tree::TerminalNode* INSERT();
    antlr4::tree::TerminalNode* OVERWRITE();
    antlr4::tree::TerminalNode* DIRECTORY();
    TableProviderContext* tableProvider();
    antlr4::tree::TerminalNode* LOCAL();
    antlr4::tree::TerminalNode* OPTIONS();
    StringLitContext* stringLit();
    PropertyListContext* propertyList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class InsertOverwriteTableContext : public InsertIntoContext {
   public:
    InsertOverwriteTableContext(InsertIntoContext* ctx);

    antlr4::tree::TerminalNode* INSERT();
    antlr4::tree::TerminalNode* OVERWRITE();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* TABLE();
    PartitionSpecContext* partitionSpec();
    IdentifierListContext* identifierList();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class InsertIntoTableContext : public InsertIntoContext {
   public:
    InsertIntoTableContext(InsertIntoContext* ctx);

    antlr4::tree::TerminalNode* INSERT();
    antlr4::tree::TerminalNode* INTO();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* TABLE();
    PartitionSpecContext* partitionSpec();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    IdentifierListContext* identifierList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  InsertIntoContext* insertInto();

  class PartitionSpecLocationContext : public antlr4::ParserRuleContext {
   public:
    PartitionSpecLocationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PartitionSpecContext* partitionSpec();
    LocationSpecContext* locationSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PartitionSpecLocationContext* partitionSpecLocation();

  class PartitionSpecContext : public antlr4::ParserRuleContext {
   public:
    PartitionSpecContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* PARTITION();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<PartitionValContext*> partitionVal();
    PartitionValContext* partitionVal(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PartitionSpecContext* partitionSpec();

  class PartitionValContext : public antlr4::ParserRuleContext {
   public:
    PartitionValContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* EQ();
    ConstantContext* constant();
    antlr4::tree::TerminalNode* DEFAULT();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PartitionValContext* partitionVal();

  class NamespaceKeywordContext : public antlr4::ParserRuleContext {
   public:
    NamespaceKeywordContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NAMESPACE();
    antlr4::tree::TerminalNode* DATABASE();
    antlr4::tree::TerminalNode* SCHEMA();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NamespaceKeywordContext* namespaceKeyword();

  class NamespacesContext : public antlr4::ParserRuleContext {
   public:
    NamespacesContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NAMESPACES();
    antlr4::tree::TerminalNode* DATABASES();
    antlr4::tree::TerminalNode* SCHEMAS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NamespacesContext* namespaces();

  class DescribeFuncNameContext : public antlr4::ParserRuleContext {
   public:
    DescribeFuncNameContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedNameContext* qualifiedName();
    StringLitContext* stringLit();
    ComparisonOperatorContext* comparisonOperator();
    ArithmeticOperatorContext* arithmeticOperator();
    PredicateOperatorContext* predicateOperator();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DescribeFuncNameContext* describeFuncName();

  class DescribeColNameContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::IdentifierContext* identifierContext = nullptr;
    std::vector<IdentifierContext*> nameParts;
    DescribeColNameContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    std::vector<antlr4::tree::TerminalNode*> DOT();
    antlr4::tree::TerminalNode* DOT(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DescribeColNameContext* describeColName();

  class CtesContext : public antlr4::ParserRuleContext {
   public:
    CtesContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WITH();
    std::vector<NamedQueryContext*> namedQuery();
    NamedQueryContext* namedQuery(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CtesContext* ctes();

  class NamedQueryContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ErrorCapturingIdentifierContext* name = nullptr;
    SqlBaseParser::IdentifierListContext* columnAliases = nullptr;
    NamedQueryContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    QueryContext* query();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier();
    antlr4::tree::TerminalNode* AS();
    IdentifierListContext* identifierList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NamedQueryContext* namedQuery();

  class TableProviderContext : public antlr4::ParserRuleContext {
   public:
    TableProviderContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* USING();
    MultipartIdentifierContext* multipartIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TableProviderContext* tableProvider();

  class CreateTableClausesContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::PropertyListContext* options = nullptr;
    SqlBaseParser::PartitionFieldListContext* partitioning = nullptr;
    SqlBaseParser::PropertyListContext* tableProps = nullptr;
    CreateTableClausesContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SkewSpecContext*> skewSpec();
    SkewSpecContext* skewSpec(size_t i);
    std::vector<BucketSpecContext*> bucketSpec();
    BucketSpecContext* bucketSpec(size_t i);
    std::vector<RowFormatContext*> rowFormat();
    RowFormatContext* rowFormat(size_t i);
    std::vector<CreateFileFormatContext*> createFileFormat();
    CreateFileFormatContext* createFileFormat(size_t i);
    std::vector<LocationSpecContext*> locationSpec();
    LocationSpecContext* locationSpec(size_t i);
    std::vector<CommentSpecContext*> commentSpec();
    CommentSpecContext* commentSpec(size_t i);
    std::vector<antlr4::tree::TerminalNode*> OPTIONS();
    antlr4::tree::TerminalNode* OPTIONS(size_t i);
    std::vector<antlr4::tree::TerminalNode*> PARTITIONED();
    antlr4::tree::TerminalNode* PARTITIONED(size_t i);
    std::vector<antlr4::tree::TerminalNode*> BY();
    antlr4::tree::TerminalNode* BY(size_t i);
    std::vector<antlr4::tree::TerminalNode*> TBLPROPERTIES();
    antlr4::tree::TerminalNode* TBLPROPERTIES(size_t i);
    std::vector<PropertyListContext*> propertyList();
    PropertyListContext* propertyList(size_t i);
    std::vector<PartitionFieldListContext*> partitionFieldList();
    PartitionFieldListContext* partitionFieldList(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CreateTableClausesContext* createTableClauses();

  class PropertyListContext : public antlr4::ParserRuleContext {
   public:
    PropertyListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<PropertyContext*> property();
    PropertyContext* property(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyListContext* propertyList();

  class PropertyContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::PropertyKeyContext* key = nullptr;
    SqlBaseParser::PropertyValueContext* value = nullptr;
    PropertyContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PropertyKeyContext* propertyKey();
    PropertyValueContext* propertyValue();
    antlr4::tree::TerminalNode* EQ();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyContext* property();

  class PropertyKeyContext : public antlr4::ParserRuleContext {
   public:
    PropertyKeyContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    std::vector<antlr4::tree::TerminalNode*> DOT();
    antlr4::tree::TerminalNode* DOT(size_t i);
    StringLitContext* stringLit();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyKeyContext* propertyKey();

  class PropertyValueContext : public antlr4::ParserRuleContext {
   public:
    PropertyValueContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    antlr4::tree::TerminalNode* DECIMAL_VALUE();
    BooleanValueContext* booleanValue();
    StringLitContext* stringLit();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyValueContext* propertyValue();

  class ConstantListContext : public antlr4::ParserRuleContext {
   public:
    ConstantListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<ConstantContext*> constant();
    ConstantContext* constant(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstantListContext* constantList();

  class NestedConstantListContext : public antlr4::ParserRuleContext {
   public:
    NestedConstantListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<ConstantListContext*> constantList();
    ConstantListContext* constantList(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NestedConstantListContext* nestedConstantList();

  class CreateFileFormatContext : public antlr4::ParserRuleContext {
   public:
    CreateFileFormatContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* STORED();
    antlr4::tree::TerminalNode* AS();
    FileFormatContext* fileFormat();
    antlr4::tree::TerminalNode* BY();
    StorageHandlerContext* storageHandler();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CreateFileFormatContext* createFileFormat();

  class FileFormatContext : public antlr4::ParserRuleContext {
   public:
    FileFormatContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    FileFormatContext() = default;
    void copyFrom(FileFormatContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class TableFileFormatContext : public FileFormatContext {
   public:
    TableFileFormatContext(FileFormatContext* ctx);

    SqlBaseParser::StringLitContext* inFmt = nullptr;
    SqlBaseParser::StringLitContext* outFmt = nullptr;
    antlr4::tree::TerminalNode* INPUTFORMAT();
    antlr4::tree::TerminalNode* OUTPUTFORMAT();
    std::vector<StringLitContext*> stringLit();
    StringLitContext* stringLit(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class GenericFileFormatContext : public FileFormatContext {
   public:
    GenericFileFormatContext(FileFormatContext* ctx);

    IdentifierContext* identifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FileFormatContext* fileFormat();

  class StorageHandlerContext : public antlr4::ParserRuleContext {
   public:
    StorageHandlerContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* WITH();
    antlr4::tree::TerminalNode* SERDEPROPERTIES();
    PropertyListContext* propertyList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StorageHandlerContext* storageHandler();

  class ResourceContext : public antlr4::ParserRuleContext {
   public:
    ResourceContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    StringLitContext* stringLit();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ResourceContext* resource();

  class DmlStatementNoWithContext : public antlr4::ParserRuleContext {
   public:
    DmlStatementNoWithContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    DmlStatementNoWithContext() = default;
    void copyFrom(DmlStatementNoWithContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class DeleteFromTableContext : public DmlStatementNoWithContext {
   public:
    DeleteFromTableContext(DmlStatementNoWithContext* ctx);

    antlr4::tree::TerminalNode* DELETE();
    antlr4::tree::TerminalNode* FROM();
    MultipartIdentifierContext* multipartIdentifier();
    TableAliasContext* tableAlias();
    WhereClauseContext* whereClause();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SingleInsertQueryContext : public DmlStatementNoWithContext {
   public:
    SingleInsertQueryContext(DmlStatementNoWithContext* ctx);

    InsertIntoContext* insertInto();
    QueryContext* query();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class MultiInsertQueryContext : public DmlStatementNoWithContext {
   public:
    MultiInsertQueryContext(DmlStatementNoWithContext* ctx);

    FromClauseContext* fromClause();
    std::vector<MultiInsertQueryBodyContext*> multiInsertQueryBody();
    MultiInsertQueryBodyContext* multiInsertQueryBody(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UpdateTableContext : public DmlStatementNoWithContext {
   public:
    UpdateTableContext(DmlStatementNoWithContext* ctx);

    antlr4::tree::TerminalNode* UPDATE();
    MultipartIdentifierContext* multipartIdentifier();
    TableAliasContext* tableAlias();
    SetClauseContext* setClause();
    WhereClauseContext* whereClause();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class MergeIntoTableContext : public DmlStatementNoWithContext {
   public:
    MergeIntoTableContext(DmlStatementNoWithContext* ctx);

    SqlBaseParser::MultipartIdentifierContext* target = nullptr;
    SqlBaseParser::TableAliasContext* targetAlias = nullptr;
    SqlBaseParser::MultipartIdentifierContext* source = nullptr;
    SqlBaseParser::QueryContext* sourceQuery = nullptr;
    SqlBaseParser::TableAliasContext* sourceAlias = nullptr;
    SqlBaseParser::BooleanExpressionContext* mergeCondition = nullptr;
    antlr4::tree::TerminalNode* MERGE();
    antlr4::tree::TerminalNode* INTO();
    antlr4::tree::TerminalNode* USING();
    antlr4::tree::TerminalNode* ON();
    std::vector<MultipartIdentifierContext*> multipartIdentifier();
    MultipartIdentifierContext* multipartIdentifier(size_t i);
    std::vector<TableAliasContext*> tableAlias();
    TableAliasContext* tableAlias(size_t i);
    BooleanExpressionContext* booleanExpression();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    QueryContext* query();
    std::vector<MatchedClauseContext*> matchedClause();
    MatchedClauseContext* matchedClause(size_t i);
    std::vector<NotMatchedClauseContext*> notMatchedClause();
    NotMatchedClauseContext* notMatchedClause(size_t i);
    std::vector<NotMatchedBySourceClauseContext*> notMatchedBySourceClause();
    NotMatchedBySourceClauseContext* notMatchedBySourceClause(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DmlStatementNoWithContext* dmlStatementNoWith();

  class QueryOrganizationContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::SortItemContext* sortItemContext = nullptr;
    std::vector<SortItemContext*> order;
    SqlBaseParser::ExpressionContext* expressionContext = nullptr;
    std::vector<ExpressionContext*> clusterBy;
    std::vector<ExpressionContext*> distributeBy;
    std::vector<SortItemContext*> sort;
    SqlBaseParser::ExpressionContext* limit = nullptr;
    SqlBaseParser::ExpressionContext* offset = nullptr;
    QueryOrganizationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ORDER();
    std::vector<antlr4::tree::TerminalNode*> BY();
    antlr4::tree::TerminalNode* BY(size_t i);
    antlr4::tree::TerminalNode* CLUSTER();
    antlr4::tree::TerminalNode* DISTRIBUTE();
    antlr4::tree::TerminalNode* SORT();
    WindowClauseContext* windowClause();
    antlr4::tree::TerminalNode* LIMIT();
    antlr4::tree::TerminalNode* OFFSET();
    std::vector<SortItemContext*> sortItem();
    SortItemContext* sortItem(size_t i);
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* ALL();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QueryOrganizationContext* queryOrganization();

  class MultiInsertQueryBodyContext : public antlr4::ParserRuleContext {
   public:
    MultiInsertQueryBodyContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    InsertIntoContext* insertInto();
    FromStatementBodyContext* fromStatementBody();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultiInsertQueryBodyContext* multiInsertQueryBody();

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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SetOperationContext : public QueryTermContext {
   public:
    SetOperationContext(QueryTermContext* ctx);

    SqlBaseParser::QueryTermContext* left = nullptr;
    antlr4::Token* op = nullptr;
    SqlBaseParser::QueryTermContext* right = nullptr;
    std::vector<QueryTermContext*> queryTerm();
    QueryTermContext* queryTerm(size_t i);
    antlr4::tree::TerminalNode* INTERSECT();
    antlr4::tree::TerminalNode* UNION();
    antlr4::tree::TerminalNode* EXCEPT();
    antlr4::tree::TerminalNode* SETMINUS();
    SetQuantifierContext* setQuantifier();

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

    antlr4::tree::TerminalNode* LEFT_PAREN();
    QueryContext* query();
    antlr4::tree::TerminalNode* RIGHT_PAREN();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class QueryPrimaryDefaultContext : public QueryPrimaryContext {
   public:
    QueryPrimaryDefaultContext(QueryPrimaryContext* ctx);

    QuerySpecificationContext* querySpecification();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class InlineTableDefault1Context : public QueryPrimaryContext {
   public:
    InlineTableDefault1Context(QueryPrimaryContext* ctx);

    InlineTableContext* inlineTable();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class FromStmtContext : public QueryPrimaryContext {
   public:
    FromStmtContext(QueryPrimaryContext* ctx);

    FromStatementContext* fromStatement();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TableContext : public QueryPrimaryContext {
   public:
    TableContext(QueryPrimaryContext* ctx);

    antlr4::tree::TerminalNode* TABLE();
    MultipartIdentifierContext* multipartIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QueryPrimaryContext* queryPrimary();

  class SortItemContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* ordering = nullptr;
    antlr4::Token* nullOrder = nullptr;
    SortItemContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* NULLS();
    antlr4::tree::TerminalNode* ASC();
    antlr4::tree::TerminalNode* DESC();
    antlr4::tree::TerminalNode* LAST();
    antlr4::tree::TerminalNode* FIRST();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SortItemContext* sortItem();

  class FromStatementContext : public antlr4::ParserRuleContext {
   public:
    FromStatementContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    FromClauseContext* fromClause();
    std::vector<FromStatementBodyContext*> fromStatementBody();
    FromStatementBodyContext* fromStatementBody(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FromStatementContext* fromStatement();

  class FromStatementBodyContext : public antlr4::ParserRuleContext {
   public:
    FromStatementBodyContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TransformClauseContext* transformClause();
    QueryOrganizationContext* queryOrganization();
    WhereClauseContext* whereClause();
    SelectClauseContext* selectClause();
    std::vector<LateralViewContext*> lateralView();
    LateralViewContext* lateralView(size_t i);
    AggregationClauseContext* aggregationClause();
    HavingClauseContext* havingClause();
    WindowClauseContext* windowClause();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FromStatementBodyContext* fromStatementBody();

  class QuerySpecificationContext : public antlr4::ParserRuleContext {
   public:
    QuerySpecificationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    QuerySpecificationContext() = default;
    void copyFrom(QuerySpecificationContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class RegularQuerySpecificationContext : public QuerySpecificationContext {
   public:
    RegularQuerySpecificationContext(QuerySpecificationContext* ctx);

    SelectClauseContext* selectClause();
    FromClauseContext* fromClause();
    std::vector<LateralViewContext*> lateralView();
    LateralViewContext* lateralView(size_t i);
    WhereClauseContext* whereClause();
    AggregationClauseContext* aggregationClause();
    HavingClauseContext* havingClause();
    WindowClauseContext* windowClause();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TransformQuerySpecificationContext : public QuerySpecificationContext {
   public:
    TransformQuerySpecificationContext(QuerySpecificationContext* ctx);

    TransformClauseContext* transformClause();
    FromClauseContext* fromClause();
    std::vector<LateralViewContext*> lateralView();
    LateralViewContext* lateralView(size_t i);
    WhereClauseContext* whereClause();
    AggregationClauseContext* aggregationClause();
    HavingClauseContext* havingClause();
    WindowClauseContext* windowClause();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QuerySpecificationContext* querySpecification();

  class TransformClauseContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* kind = nullptr;
    SqlBaseParser::RowFormatContext* inRowFormat = nullptr;
    SqlBaseParser::StringLitContext* recordWriter = nullptr;
    SqlBaseParser::StringLitContext* script = nullptr;
    SqlBaseParser::RowFormatContext* outRowFormat = nullptr;
    SqlBaseParser::StringLitContext* recordReader = nullptr;
    TransformClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* USING();
    std::vector<StringLitContext*> stringLit();
    StringLitContext* stringLit(size_t i);
    antlr4::tree::TerminalNode* SELECT();
    std::vector<antlr4::tree::TerminalNode*> LEFT_PAREN();
    antlr4::tree::TerminalNode* LEFT_PAREN(size_t i);
    ExpressionSeqContext* expressionSeq();
    std::vector<antlr4::tree::TerminalNode*> RIGHT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN(size_t i);
    antlr4::tree::TerminalNode* TRANSFORM();
    antlr4::tree::TerminalNode* MAP();
    antlr4::tree::TerminalNode* REDUCE();
    antlr4::tree::TerminalNode* RECORDWRITER();
    antlr4::tree::TerminalNode* AS();
    antlr4::tree::TerminalNode* RECORDREADER();
    std::vector<RowFormatContext*> rowFormat();
    RowFormatContext* rowFormat(size_t i);
    SetQuantifierContext* setQuantifier();
    IdentifierSeqContext* identifierSeq();
    ColTypeListContext* colTypeList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TransformClauseContext* transformClause();

  class SelectClauseContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::HintContext* hintContext = nullptr;
    std::vector<HintContext*> hints;
    SelectClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SELECT();
    NamedExpressionSeqContext* namedExpressionSeq();
    SetQuantifierContext* setQuantifier();
    std::vector<HintContext*> hint();
    HintContext* hint(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SelectClauseContext* selectClause();

  class SetClauseContext : public antlr4::ParserRuleContext {
   public:
    SetClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SET();
    AssignmentListContext* assignmentList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SetClauseContext* setClause();

  class MatchedClauseContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::BooleanExpressionContext* matchedCond = nullptr;
    MatchedClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WHEN();
    antlr4::tree::TerminalNode* MATCHED();
    antlr4::tree::TerminalNode* THEN();
    MatchedActionContext* matchedAction();
    antlr4::tree::TerminalNode* AND();
    BooleanExpressionContext* booleanExpression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MatchedClauseContext* matchedClause();

  class NotMatchedClauseContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::BooleanExpressionContext* notMatchedCond = nullptr;
    NotMatchedClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WHEN();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* MATCHED();
    antlr4::tree::TerminalNode* THEN();
    NotMatchedActionContext* notMatchedAction();
    antlr4::tree::TerminalNode* BY();
    antlr4::tree::TerminalNode* TARGET();
    antlr4::tree::TerminalNode* AND();
    BooleanExpressionContext* booleanExpression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NotMatchedClauseContext* notMatchedClause();

  class NotMatchedBySourceClauseContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::BooleanExpressionContext* notMatchedBySourceCond = nullptr;
    NotMatchedBySourceClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WHEN();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* MATCHED();
    antlr4::tree::TerminalNode* BY();
    antlr4::tree::TerminalNode* SOURCE();
    antlr4::tree::TerminalNode* THEN();
    NotMatchedBySourceActionContext* notMatchedBySourceAction();
    antlr4::tree::TerminalNode* AND();
    BooleanExpressionContext* booleanExpression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NotMatchedBySourceClauseContext* notMatchedBySourceClause();

  class MatchedActionContext : public antlr4::ParserRuleContext {
   public:
    MatchedActionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* DELETE();
    antlr4::tree::TerminalNode* UPDATE();
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* ASTERISK();
    AssignmentListContext* assignmentList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MatchedActionContext* matchedAction();

  class NotMatchedActionContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::MultipartIdentifierListContext* columns = nullptr;
    NotMatchedActionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INSERT();
    antlr4::tree::TerminalNode* ASTERISK();
    std::vector<antlr4::tree::TerminalNode*> LEFT_PAREN();
    antlr4::tree::TerminalNode* LEFT_PAREN(size_t i);
    std::vector<antlr4::tree::TerminalNode*> RIGHT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN(size_t i);
    antlr4::tree::TerminalNode* VALUES();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    MultipartIdentifierListContext* multipartIdentifierList();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NotMatchedActionContext* notMatchedAction();

  class NotMatchedBySourceActionContext : public antlr4::ParserRuleContext {
   public:
    NotMatchedBySourceActionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* DELETE();
    antlr4::tree::TerminalNode* UPDATE();
    antlr4::tree::TerminalNode* SET();
    AssignmentListContext* assignmentList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NotMatchedBySourceActionContext* notMatchedBySourceAction();

  class AssignmentListContext : public antlr4::ParserRuleContext {
   public:
    AssignmentListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AssignmentContext*> assignment();
    AssignmentContext* assignment(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AssignmentListContext* assignmentList();

  class AssignmentContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::MultipartIdentifierContext* key = nullptr;
    SqlBaseParser::ExpressionContext* value = nullptr;
    AssignmentContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* EQ();
    MultipartIdentifierContext* multipartIdentifier();
    ExpressionContext* expression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AssignmentContext* assignment();

  class WhereClauseContext : public antlr4::ParserRuleContext {
   public:
    WhereClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WHERE();
    BooleanExpressionContext* booleanExpression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WhereClauseContext* whereClause();

  class HavingClauseContext : public antlr4::ParserRuleContext {
   public:
    HavingClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* HAVING();
    BooleanExpressionContext* booleanExpression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  HavingClauseContext* havingClause();

  class HintContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::HintStatementContext* hintStatementContext = nullptr;
    std::vector<HintStatementContext*> hintStatements;
    HintContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* HENT_START();
    antlr4::tree::TerminalNode* HENT_END();
    std::vector<HintStatementContext*> hintStatement();
    HintStatementContext* hintStatement(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  HintContext* hint();

  class HintStatementContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::IdentifierContext* hintName = nullptr;
    SqlBaseParser::PrimaryExpressionContext* primaryExpressionContext = nullptr;
    std::vector<PrimaryExpressionContext*> parameters;
    HintStatementContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<PrimaryExpressionContext*> primaryExpression();
    PrimaryExpressionContext* primaryExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  HintStatementContext* hintStatement();

  class FromClauseContext : public antlr4::ParserRuleContext {
   public:
    FromClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* FROM();
    std::vector<RelationContext*> relation();
    RelationContext* relation(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    std::vector<LateralViewContext*> lateralView();
    LateralViewContext* lateralView(size_t i);
    PivotClauseContext* pivotClause();
    UnpivotClauseContext* unpivotClause();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FromClauseContext* fromClause();

  class TemporalClauseContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ValueExpressionContext* timestamp = nullptr;
    TemporalClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* AS();
    antlr4::tree::TerminalNode* OF();
    VersionContext* version();
    antlr4::tree::TerminalNode* SYSTEM_VERSION();
    antlr4::tree::TerminalNode* VERSION();
    antlr4::tree::TerminalNode* FOR();
    antlr4::tree::TerminalNode* SYSTEM_TIME();
    antlr4::tree::TerminalNode* TIMESTAMP();
    ValueExpressionContext* valueExpression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TemporalClauseContext* temporalClause();

  class AggregationClauseContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::GroupByClauseContext* groupByClauseContext = nullptr;
    std::vector<GroupByClauseContext*> groupingExpressionsWithGroupingAnalytics;
    SqlBaseParser::ExpressionContext* expressionContext = nullptr;
    std::vector<ExpressionContext*> groupingExpressions;
    antlr4::Token* kind = nullptr;
    AggregationClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* GROUP();
    antlr4::tree::TerminalNode* BY();
    std::vector<GroupByClauseContext*> groupByClause();
    GroupByClauseContext* groupByClause(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* WITH();
    antlr4::tree::TerminalNode* SETS();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<GroupingSetContext*> groupingSet();
    GroupingSetContext* groupingSet(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* ROLLUP();
    antlr4::tree::TerminalNode* CUBE();
    antlr4::tree::TerminalNode* GROUPING();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AggregationClauseContext* aggregationClause();

  class GroupByClauseContext : public antlr4::ParserRuleContext {
   public:
    GroupByClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GroupingAnalyticsContext* groupingAnalytics();
    ExpressionContext* expression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupByClauseContext* groupByClause();

  class GroupingAnalyticsContext : public antlr4::ParserRuleContext {
   public:
    GroupingAnalyticsContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<GroupingSetContext*> groupingSet();
    GroupingSetContext* groupingSet(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* ROLLUP();
    antlr4::tree::TerminalNode* CUBE();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode* GROUPING();
    antlr4::tree::TerminalNode* SETS();
    std::vector<GroupingElementContext*> groupingElement();
    GroupingElementContext* groupingElement(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupingAnalyticsContext* groupingAnalytics();

  class GroupingElementContext : public antlr4::ParserRuleContext {
   public:
    GroupingElementContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GroupingAnalyticsContext* groupingAnalytics();
    GroupingSetContext* groupingSet();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupingElementContext* groupingElement();

  class GroupingSetContext : public antlr4::ParserRuleContext {
   public:
    GroupingSetContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupingSetContext* groupingSet();

  class PivotClauseContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::NamedExpressionSeqContext* aggregates = nullptr;
    SqlBaseParser::PivotValueContext* pivotValueContext = nullptr;
    std::vector<PivotValueContext*> pivotValues;
    PivotClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* PIVOT();
    std::vector<antlr4::tree::TerminalNode*> LEFT_PAREN();
    antlr4::tree::TerminalNode* LEFT_PAREN(size_t i);
    antlr4::tree::TerminalNode* FOR();
    PivotColumnContext* pivotColumn();
    antlr4::tree::TerminalNode* IN();
    std::vector<antlr4::tree::TerminalNode*> RIGHT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN(size_t i);
    NamedExpressionSeqContext* namedExpressionSeq();
    std::vector<PivotValueContext*> pivotValue();
    PivotValueContext* pivotValue(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PivotClauseContext* pivotClause();

  class PivotColumnContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::IdentifierContext* identifierContext = nullptr;
    std::vector<IdentifierContext*> identifiers;
    PivotColumnContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PivotColumnContext* pivotColumn();

  class PivotValueContext : public antlr4::ParserRuleContext {
   public:
    PivotValueContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* AS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PivotValueContext* pivotValue();

  class UnpivotClauseContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::UnpivotNullClauseContext* nullOperator = nullptr;
    SqlBaseParser::UnpivotOperatorContext* op = nullptr;
    UnpivotClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* UNPIVOT();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    UnpivotOperatorContext* unpivotOperator();
    IdentifierContext* identifier();
    UnpivotNullClauseContext* unpivotNullClause();
    antlr4::tree::TerminalNode* AS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotClauseContext* unpivotClause();

  class UnpivotNullClauseContext : public antlr4::ParserRuleContext {
   public:
    UnpivotNullClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NULLS();
    antlr4::tree::TerminalNode* INCLUDE();
    antlr4::tree::TerminalNode* EXCLUDE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotNullClauseContext* unpivotNullClause();

  class UnpivotOperatorContext : public antlr4::ParserRuleContext {
   public:
    UnpivotOperatorContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UnpivotSingleValueColumnClauseContext* unpivotSingleValueColumnClause();
    UnpivotMultiValueColumnClauseContext* unpivotMultiValueColumnClause();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotOperatorContext* unpivotOperator();

  class UnpivotSingleValueColumnClauseContext
      : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::UnpivotColumnAndAliasContext* unpivotColumnAndAliasContext =
        nullptr;
    std::vector<UnpivotColumnAndAliasContext*> unpivotColumns;
    UnpivotSingleValueColumnClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UnpivotValueColumnContext* unpivotValueColumn();
    antlr4::tree::TerminalNode* FOR();
    UnpivotNameColumnContext* unpivotNameColumn();
    antlr4::tree::TerminalNode* IN();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<UnpivotColumnAndAliasContext*> unpivotColumnAndAlias();
    UnpivotColumnAndAliasContext* unpivotColumnAndAlias(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotSingleValueColumnClauseContext* unpivotSingleValueColumnClause();

  class UnpivotMultiValueColumnClauseContext
      : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::UnpivotValueColumnContext* unpivotValueColumnContext =
        nullptr;
    std::vector<UnpivotValueColumnContext*> unpivotValueColumns;
    SqlBaseParser::UnpivotColumnSetContext* unpivotColumnSetContext = nullptr;
    std::vector<UnpivotColumnSetContext*> unpivotColumnSets;
    UnpivotMultiValueColumnClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode*> LEFT_PAREN();
    antlr4::tree::TerminalNode* LEFT_PAREN(size_t i);
    std::vector<antlr4::tree::TerminalNode*> RIGHT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN(size_t i);
    antlr4::tree::TerminalNode* FOR();
    UnpivotNameColumnContext* unpivotNameColumn();
    antlr4::tree::TerminalNode* IN();
    std::vector<UnpivotValueColumnContext*> unpivotValueColumn();
    UnpivotValueColumnContext* unpivotValueColumn(size_t i);
    std::vector<UnpivotColumnSetContext*> unpivotColumnSet();
    UnpivotColumnSetContext* unpivotColumnSet(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotMultiValueColumnClauseContext* unpivotMultiValueColumnClause();

  class UnpivotColumnSetContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::UnpivotColumnContext* unpivotColumnContext = nullptr;
    std::vector<UnpivotColumnContext*> unpivotColumns;
    UnpivotColumnSetContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<UnpivotColumnContext*> unpivotColumn();
    UnpivotColumnContext* unpivotColumn(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    UnpivotAliasContext* unpivotAlias();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotColumnSetContext* unpivotColumnSet();

  class UnpivotValueColumnContext : public antlr4::ParserRuleContext {
   public:
    UnpivotValueColumnContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotValueColumnContext* unpivotValueColumn();

  class UnpivotNameColumnContext : public antlr4::ParserRuleContext {
   public:
    UnpivotNameColumnContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotNameColumnContext* unpivotNameColumn();

  class UnpivotColumnAndAliasContext : public antlr4::ParserRuleContext {
   public:
    UnpivotColumnAndAliasContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UnpivotColumnContext* unpivotColumn();
    UnpivotAliasContext* unpivotAlias();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotColumnAndAliasContext* unpivotColumnAndAlias();

  class UnpivotColumnContext : public antlr4::ParserRuleContext {
   public:
    UnpivotColumnContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MultipartIdentifierContext* multipartIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotColumnContext* unpivotColumn();

  class UnpivotAliasContext : public antlr4::ParserRuleContext {
   public:
    UnpivotAliasContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* AS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnpivotAliasContext* unpivotAlias();

  class LateralViewContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::IdentifierContext* tblName = nullptr;
    SqlBaseParser::IdentifierContext* identifierContext = nullptr;
    std::vector<IdentifierContext*> colName;
    LateralViewContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LATERAL();
    antlr4::tree::TerminalNode* VIEW();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    antlr4::tree::TerminalNode* OUTER();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode* AS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LateralViewContext* lateralView();

  class SetQuantifierContext : public antlr4::ParserRuleContext {
   public:
    SetQuantifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* DISTINCT();
    antlr4::tree::TerminalNode* ALL();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SetQuantifierContext* setQuantifier();

  class RelationContext : public antlr4::ParserRuleContext {
   public:
    RelationContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RelationPrimaryContext* relationPrimary();
    antlr4::tree::TerminalNode* LATERAL();
    std::vector<RelationExtensionContext*> relationExtension();
    RelationExtensionContext* relationExtension(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RelationContext* relation();

  class RelationExtensionContext : public antlr4::ParserRuleContext {
   public:
    RelationExtensionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    JoinRelationContext* joinRelation();
    PivotClauseContext* pivotClause();
    UnpivotClauseContext* unpivotClause();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RelationExtensionContext* relationExtension();

  class JoinRelationContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::RelationPrimaryContext* right = nullptr;
    JoinRelationContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* JOIN();
    RelationPrimaryContext* relationPrimary();
    JoinTypeContext* joinType();
    antlr4::tree::TerminalNode* LATERAL();
    JoinCriteriaContext* joinCriteria();
    antlr4::tree::TerminalNode* NATURAL();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  JoinRelationContext* joinRelation();

  class JoinTypeContext : public antlr4::ParserRuleContext {
   public:
    JoinTypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INNER();
    antlr4::tree::TerminalNode* CROSS();
    antlr4::tree::TerminalNode* LEFT();
    antlr4::tree::TerminalNode* OUTER();
    antlr4::tree::TerminalNode* SEMI();
    antlr4::tree::TerminalNode* RIGHT();
    antlr4::tree::TerminalNode* FULL();
    antlr4::tree::TerminalNode* ANTI();

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
    IdentifierListContext* identifierList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  JoinCriteriaContext* joinCriteria();

  class SampleContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* seed = nullptr;
    SampleContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* TABLESAMPLE();
    std::vector<antlr4::tree::TerminalNode*> LEFT_PAREN();
    antlr4::tree::TerminalNode* LEFT_PAREN(size_t i);
    std::vector<antlr4::tree::TerminalNode*> RIGHT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN(size_t i);
    SampleMethodContext* sampleMethod();
    antlr4::tree::TerminalNode* REPEATABLE();
    antlr4::tree::TerminalNode* INTEGER_VALUE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SampleContext* sample();

  class SampleMethodContext : public antlr4::ParserRuleContext {
   public:
    SampleMethodContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    SampleMethodContext() = default;
    void copyFrom(SampleMethodContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class SampleByRowsContext : public SampleMethodContext {
   public:
    SampleByRowsContext(SampleMethodContext* ctx);

    ExpressionContext* expression();
    antlr4::tree::TerminalNode* ROWS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SampleByPercentileContext : public SampleMethodContext {
   public:
    SampleByPercentileContext(SampleMethodContext* ctx);

    antlr4::Token* negativeSign = nullptr;
    antlr4::Token* percentage = nullptr;
    antlr4::tree::TerminalNode* PERCENTLIT();
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    antlr4::tree::TerminalNode* DECIMAL_VALUE();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SampleByBucketContext : public SampleMethodContext {
   public:
    SampleByBucketContext(SampleMethodContext* ctx);

    antlr4::Token* sampleType = nullptr;
    antlr4::Token* numerator = nullptr;
    antlr4::Token* denominator = nullptr;
    antlr4::tree::TerminalNode* OUT();
    antlr4::tree::TerminalNode* OF();
    antlr4::tree::TerminalNode* BUCKET();
    std::vector<antlr4::tree::TerminalNode*> INTEGER_VALUE();
    antlr4::tree::TerminalNode* INTEGER_VALUE(size_t i);
    antlr4::tree::TerminalNode* ON();
    IdentifierContext* identifier();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SampleByBytesContext : public SampleMethodContext {
   public:
    SampleByBytesContext(SampleMethodContext* ctx);

    SqlBaseParser::ExpressionContext* bytes = nullptr;
    ExpressionContext* expression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SampleMethodContext* sampleMethod();

  class IdentifierListContext : public antlr4::ParserRuleContext {
   public:
    IdentifierListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    IdentifierSeqContext* identifierSeq();
    antlr4::tree::TerminalNode* RIGHT_PAREN();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IdentifierListContext* identifierList();

  class IdentifierSeqContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ErrorCapturingIdentifierContext*
        errorCapturingIdentifierContext = nullptr;
    std::vector<ErrorCapturingIdentifierContext*> ident;
    IdentifierSeqContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ErrorCapturingIdentifierContext*> errorCapturingIdentifier();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IdentifierSeqContext* identifierSeq();

  class OrderedIdentifierListContext : public antlr4::ParserRuleContext {
   public:
    OrderedIdentifierListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<OrderedIdentifierContext*> orderedIdentifier();
    OrderedIdentifierContext* orderedIdentifier(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  OrderedIdentifierListContext* orderedIdentifierList();

  class OrderedIdentifierContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ErrorCapturingIdentifierContext* ident = nullptr;
    antlr4::Token* ordering = nullptr;
    OrderedIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ErrorCapturingIdentifierContext* errorCapturingIdentifier();
    antlr4::tree::TerminalNode* ASC();
    antlr4::tree::TerminalNode* DESC();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  OrderedIdentifierContext* orderedIdentifier();

  class IdentifierCommentListContext : public antlr4::ParserRuleContext {
   public:
    IdentifierCommentListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<IdentifierCommentContext*> identifierComment();
    IdentifierCommentContext* identifierComment(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IdentifierCommentListContext* identifierCommentList();

  class IdentifierCommentContext : public antlr4::ParserRuleContext {
   public:
    IdentifierCommentContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    CommentSpecContext* commentSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IdentifierCommentContext* identifierComment();

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

  class TableValuedFunctionContext : public RelationPrimaryContext {
   public:
    TableValuedFunctionContext(RelationPrimaryContext* ctx);

    FunctionTableContext* functionTable();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class InlineTableDefault2Context : public RelationPrimaryContext {
   public:
    InlineTableDefault2Context(RelationPrimaryContext* ctx);

    InlineTableContext* inlineTable();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AliasedRelationContext : public RelationPrimaryContext {
   public:
    AliasedRelationContext(RelationPrimaryContext* ctx);

    antlr4::tree::TerminalNode* LEFT_PAREN();
    RelationContext* relation();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    TableAliasContext* tableAlias();
    SampleContext* sample();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class AliasedQueryContext : public RelationPrimaryContext {
   public:
    AliasedQueryContext(RelationPrimaryContext* ctx);

    antlr4::tree::TerminalNode* LEFT_PAREN();
    QueryContext* query();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    TableAliasContext* tableAlias();
    SampleContext* sample();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TableNameContext : public RelationPrimaryContext {
   public:
    TableNameContext(RelationPrimaryContext* ctx);

    MultipartIdentifierContext* multipartIdentifier();
    TableAliasContext* tableAlias();
    TemporalClauseContext* temporalClause();
    SampleContext* sample();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RelationPrimaryContext* relationPrimary();

  class InlineTableContext : public antlr4::ParserRuleContext {
   public:
    InlineTableContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* VALUES();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    TableAliasContext* tableAlias();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  InlineTableContext* inlineTable();

  class FunctionTableContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::FunctionNameContext* funcName = nullptr;
    FunctionTableContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    TableAliasContext* tableAlias();
    FunctionNameContext* functionName();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FunctionTableContext* functionTable();

  class TableAliasContext : public antlr4::ParserRuleContext {
   public:
    TableAliasContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StrictIdentifierContext* strictIdentifier();
    antlr4::tree::TerminalNode* AS();
    IdentifierListContext* identifierList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TableAliasContext* tableAlias();

  class RowFormatContext : public antlr4::ParserRuleContext {
   public:
    RowFormatContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    RowFormatContext() = default;
    void copyFrom(RowFormatContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class RowFormatSerdeContext : public RowFormatContext {
   public:
    RowFormatSerdeContext(RowFormatContext* ctx);

    SqlBaseParser::StringLitContext* name = nullptr;
    SqlBaseParser::PropertyListContext* props = nullptr;
    antlr4::tree::TerminalNode* ROW();
    antlr4::tree::TerminalNode* FORMAT();
    antlr4::tree::TerminalNode* SERDE();
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* WITH();
    antlr4::tree::TerminalNode* SERDEPROPERTIES();
    PropertyListContext* propertyList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RowFormatDelimitedContext : public RowFormatContext {
   public:
    RowFormatDelimitedContext(RowFormatContext* ctx);

    SqlBaseParser::StringLitContext* fieldsTerminatedBy = nullptr;
    SqlBaseParser::StringLitContext* escapedBy = nullptr;
    SqlBaseParser::StringLitContext* collectionItemsTerminatedBy = nullptr;
    SqlBaseParser::StringLitContext* keysTerminatedBy = nullptr;
    SqlBaseParser::StringLitContext* linesSeparatedBy = nullptr;
    SqlBaseParser::StringLitContext* nullDefinedAs = nullptr;
    antlr4::tree::TerminalNode* ROW();
    antlr4::tree::TerminalNode* FORMAT();
    antlr4::tree::TerminalNode* DELIMITED();
    antlr4::tree::TerminalNode* FIELDS();
    std::vector<antlr4::tree::TerminalNode*> TERMINATED();
    antlr4::tree::TerminalNode* TERMINATED(size_t i);
    std::vector<antlr4::tree::TerminalNode*> BY();
    antlr4::tree::TerminalNode* BY(size_t i);
    antlr4::tree::TerminalNode* COLLECTION();
    antlr4::tree::TerminalNode* ITEMS();
    antlr4::tree::TerminalNode* MAP();
    antlr4::tree::TerminalNode* KEYS();
    antlr4::tree::TerminalNode* LINES();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    antlr4::tree::TerminalNode* DEFINED();
    antlr4::tree::TerminalNode* AS();
    std::vector<StringLitContext*> stringLit();
    StringLitContext* stringLit(size_t i);
    antlr4::tree::TerminalNode* ESCAPED();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RowFormatContext* rowFormat();

  class MultipartIdentifierListContext : public antlr4::ParserRuleContext {
   public:
    MultipartIdentifierListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<MultipartIdentifierContext*> multipartIdentifier();
    MultipartIdentifierContext* multipartIdentifier(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultipartIdentifierListContext* multipartIdentifierList();

  class MultipartIdentifierContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ErrorCapturingIdentifierContext*
        errorCapturingIdentifierContext = nullptr;
    std::vector<ErrorCapturingIdentifierContext*> parts;
    MultipartIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ErrorCapturingIdentifierContext*> errorCapturingIdentifier();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier(size_t i);
    std::vector<antlr4::tree::TerminalNode*> DOT();
    antlr4::tree::TerminalNode* DOT(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultipartIdentifierContext* multipartIdentifier();

  class MultipartIdentifierPropertyListContext
      : public antlr4::ParserRuleContext {
   public:
    MultipartIdentifierPropertyListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<MultipartIdentifierPropertyContext*>
    multipartIdentifierProperty();
    MultipartIdentifierPropertyContext* multipartIdentifierProperty(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultipartIdentifierPropertyListContext* multipartIdentifierPropertyList();

  class MultipartIdentifierPropertyContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::PropertyListContext* options = nullptr;
    MultipartIdentifierPropertyContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* OPTIONS();
    PropertyListContext* propertyList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultipartIdentifierPropertyContext* multipartIdentifierProperty();

  class TableIdentifierContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ErrorCapturingIdentifierContext* db = nullptr;
    SqlBaseParser::ErrorCapturingIdentifierContext* table = nullptr;
    TableIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ErrorCapturingIdentifierContext*> errorCapturingIdentifier();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier(size_t i);
    antlr4::tree::TerminalNode* DOT();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TableIdentifierContext* tableIdentifier();

  class FunctionIdentifierContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ErrorCapturingIdentifierContext* db = nullptr;
    SqlBaseParser::ErrorCapturingIdentifierContext* function = nullptr;
    FunctionIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ErrorCapturingIdentifierContext*> errorCapturingIdentifier();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier(size_t i);
    antlr4::tree::TerminalNode* DOT();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FunctionIdentifierContext* functionIdentifier();

  class NamedExpressionContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ErrorCapturingIdentifierContext* name = nullptr;
    NamedExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    IdentifierListContext* identifierList();
    antlr4::tree::TerminalNode* AS();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NamedExpressionContext* namedExpression();

  class NamedExpressionSeqContext : public antlr4::ParserRuleContext {
   public:
    NamedExpressionSeqContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NamedExpressionContext*> namedExpression();
    NamedExpressionContext* namedExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NamedExpressionSeqContext* namedExpressionSeq();

  class PartitionFieldListContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::PartitionFieldContext* partitionFieldContext = nullptr;
    std::vector<PartitionFieldContext*> fields;
    PartitionFieldListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<PartitionFieldContext*> partitionField();
    PartitionFieldContext* partitionField(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PartitionFieldListContext* partitionFieldList();

  class PartitionFieldContext : public antlr4::ParserRuleContext {
   public:
    PartitionFieldContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    PartitionFieldContext() = default;
    void copyFrom(PartitionFieldContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class PartitionColumnContext : public PartitionFieldContext {
   public:
    PartitionColumnContext(PartitionFieldContext* ctx);

    ColTypeContext* colType();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class PartitionTransformContext : public PartitionFieldContext {
   public:
    PartitionTransformContext(PartitionFieldContext* ctx);

    TransformContext* transform();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PartitionFieldContext* partitionField();

  class TransformContext : public antlr4::ParserRuleContext {
   public:
    TransformContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    TransformContext() = default;
    void copyFrom(TransformContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class IdentityTransformContext : public TransformContext {
   public:
    IdentityTransformContext(TransformContext* ctx);

    QualifiedNameContext* qualifiedName();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ApplyTransformContext : public TransformContext {
   public:
    ApplyTransformContext(TransformContext* ctx);

    SqlBaseParser::IdentifierContext* transformName = nullptr;
    SqlBaseParser::TransformArgumentContext* transformArgumentContext = nullptr;
    std::vector<TransformArgumentContext*> argument;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    IdentifierContext* identifier();
    std::vector<TransformArgumentContext*> transformArgument();
    TransformArgumentContext* transformArgument(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TransformContext* transform();

  class TransformArgumentContext : public antlr4::ParserRuleContext {
   public:
    TransformArgumentContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedNameContext* qualifiedName();
    ConstantContext* constant();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TransformArgumentContext* transformArgument();

  class ExpressionContext : public antlr4::ParserRuleContext {
   public:
    ExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BooleanExpressionContext* booleanExpression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ExpressionContext* expression();

  class ExpressionSeqContext : public antlr4::ParserRuleContext {
   public:
    ExpressionSeqContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ExpressionSeqContext* expressionSeq();

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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class PredicatedContext : public BooleanExpressionContext {
   public:
    PredicatedContext(BooleanExpressionContext* ctx);

    ValueExpressionContext* valueExpression();
    PredicateContext* predicate();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ExistsContext : public BooleanExpressionContext {
   public:
    ExistsContext(BooleanExpressionContext* ctx);

    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    QueryContext* query();
    antlr4::tree::TerminalNode* RIGHT_PAREN();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LogicalBinaryContext : public BooleanExpressionContext {
   public:
    LogicalBinaryContext(BooleanExpressionContext* ctx);

    SqlBaseParser::BooleanExpressionContext* left = nullptr;
    antlr4::Token* op = nullptr;
    SqlBaseParser::BooleanExpressionContext* right = nullptr;
    std::vector<BooleanExpressionContext*> booleanExpression();
    BooleanExpressionContext* booleanExpression(size_t i);
    antlr4::tree::TerminalNode* AND();
    antlr4::tree::TerminalNode* OR();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BooleanExpressionContext* booleanExpression();
  BooleanExpressionContext* booleanExpression(int precedence);
  class PredicateContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* kind = nullptr;
    SqlBaseParser::ValueExpressionContext* lower = nullptr;
    SqlBaseParser::ValueExpressionContext* upper = nullptr;
    SqlBaseParser::ValueExpressionContext* pattern = nullptr;
    antlr4::Token* quantifier = nullptr;
    SqlBaseParser::StringLitContext* escapeChar = nullptr;
    SqlBaseParser::ValueExpressionContext* right = nullptr;
    PredicateContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* AND();
    antlr4::tree::TerminalNode* BETWEEN();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* IN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    QueryContext* query();
    antlr4::tree::TerminalNode* RLIKE();
    antlr4::tree::TerminalNode* LIKE();
    antlr4::tree::TerminalNode* ILIKE();
    antlr4::tree::TerminalNode* ANY();
    antlr4::tree::TerminalNode* SOME();
    antlr4::tree::TerminalNode* ALL();
    antlr4::tree::TerminalNode* ESCAPE();
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* IS();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    antlr4::tree::TerminalNode* TRUE();
    antlr4::tree::TerminalNode* FALSE();
    antlr4::tree::TerminalNode* UNKNOWN();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* DISTINCT();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PredicateContext* predicate();

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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ComparisonContext : public ValueExpressionContext {
   public:
    ComparisonContext(ValueExpressionContext* ctx);

    SqlBaseParser::ValueExpressionContext* left = nullptr;
    SqlBaseParser::ValueExpressionContext* right = nullptr;
    ComparisonOperatorContext* comparisonOperator();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ArithmeticBinaryContext : public ValueExpressionContext {
   public:
    ArithmeticBinaryContext(ValueExpressionContext* ctx);

    SqlBaseParser::ValueExpressionContext* left = nullptr;
    antlr4::Token* op = nullptr;
    SqlBaseParser::ValueExpressionContext* right = nullptr;
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* ASTERISK();
    antlr4::tree::TerminalNode* SLASH();
    antlr4::tree::TerminalNode* PERCENT();
    antlr4::tree::TerminalNode* DIV();
    antlr4::tree::TerminalNode* PLUS();
    antlr4::tree::TerminalNode* MINUS();
    antlr4::tree::TerminalNode* CONCAT_PIPE();
    antlr4::tree::TerminalNode* AMPERSAND();
    antlr4::tree::TerminalNode* HAT();
    antlr4::tree::TerminalNode* PIPE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ArithmeticUnaryContext : public ValueExpressionContext {
   public:
    ArithmeticUnaryContext(ValueExpressionContext* ctx);

    antlr4::Token* op = nullptr;
    ValueExpressionContext* valueExpression();
    antlr4::tree::TerminalNode* MINUS();
    antlr4::tree::TerminalNode* PLUS();
    antlr4::tree::TerminalNode* TILDE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ValueExpressionContext* valueExpression();
  ValueExpressionContext* valueExpression(int precedence);
  class DatetimeUnitContext : public antlr4::ParserRuleContext {
   public:
    DatetimeUnitContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* YEAR();
    antlr4::tree::TerminalNode* QUARTER();
    antlr4::tree::TerminalNode* MONTH();
    antlr4::tree::TerminalNode* WEEK();
    antlr4::tree::TerminalNode* DAY();
    antlr4::tree::TerminalNode* DAYOFYEAR();
    antlr4::tree::TerminalNode* HOUR();
    antlr4::tree::TerminalNode* MINUTE();
    antlr4::tree::TerminalNode* SECOND();
    antlr4::tree::TerminalNode* MILLISECOND();
    antlr4::tree::TerminalNode* MICROSECOND();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DatetimeUnitContext* datetimeUnit();

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

  class StructContext : public PrimaryExpressionContext {
   public:
    StructContext(PrimaryExpressionContext* ctx);

    SqlBaseParser::NamedExpressionContext* namedExpressionContext = nullptr;
    std::vector<NamedExpressionContext*> argument;
    antlr4::tree::TerminalNode* STRUCT();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<NamedExpressionContext*> namedExpression();
    NamedExpressionContext* namedExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DereferenceContext : public PrimaryExpressionContext {
   public:
    DereferenceContext(PrimaryExpressionContext* ctx);

    SqlBaseParser::PrimaryExpressionContext* base = nullptr;
    SqlBaseParser::IdentifierContext* fieldName = nullptr;
    antlr4::tree::TerminalNode* DOT();
    PrimaryExpressionContext* primaryExpression();
    IdentifierContext* identifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TimestampaddContext : public PrimaryExpressionContext {
   public:
    TimestampaddContext(PrimaryExpressionContext* ctx);

    antlr4::Token* name = nullptr;
    SqlBaseParser::DatetimeUnitContext* unit = nullptr;
    SqlBaseParser::ValueExpressionContext* unitsAmount = nullptr;
    SqlBaseParser::ValueExpressionContext* timestamp = nullptr;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    DatetimeUnitContext* datetimeUnit();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* TIMESTAMPADD();
    antlr4::tree::TerminalNode* DATEADD();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SubstringContext : public PrimaryExpressionContext {
   public:
    SubstringContext(PrimaryExpressionContext* ctx);

    SqlBaseParser::ValueExpressionContext* str = nullptr;
    SqlBaseParser::ValueExpressionContext* pos = nullptr;
    SqlBaseParser::ValueExpressionContext* len = nullptr;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* SUBSTR();
    antlr4::tree::TerminalNode* SUBSTRING();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* FROM();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode* FOR();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CastContext : public PrimaryExpressionContext {
   public:
    CastContext(PrimaryExpressionContext* ctx);

    antlr4::Token* name = nullptr;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* AS();
    DataTypeContext* dataType();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* CAST();
    antlr4::tree::TerminalNode* TRY_CAST();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LambdaContext : public PrimaryExpressionContext {
   public:
    LambdaContext(PrimaryExpressionContext* ctx);

    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    antlr4::tree::TerminalNode* ARROW();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ParenthesizedExpressionContext : public PrimaryExpressionContext {
   public:
    ParenthesizedExpressionContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* LEFT_PAREN();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* RIGHT_PAREN();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class Any_valueContext : public PrimaryExpressionContext {
   public:
    Any_valueContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* ANY_VALUE();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* IGNORE();
    antlr4::tree::TerminalNode* NULLS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TrimContext : public PrimaryExpressionContext {
   public:
    TrimContext(PrimaryExpressionContext* ctx);

    antlr4::Token* trimOption = nullptr;
    SqlBaseParser::ValueExpressionContext* trimStr = nullptr;
    SqlBaseParser::ValueExpressionContext* srcStr = nullptr;
    antlr4::tree::TerminalNode* TRIM();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* BOTH();
    antlr4::tree::TerminalNode* LEADING();
    antlr4::tree::TerminalNode* TRAILING();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SimpleCaseContext : public PrimaryExpressionContext {
   public:
    SimpleCaseContext(PrimaryExpressionContext* ctx);

    SqlBaseParser::ExpressionContext* value = nullptr;
    SqlBaseParser::ExpressionContext* elseExpression = nullptr;
    antlr4::tree::TerminalNode* CASE();
    antlr4::tree::TerminalNode* END();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    std::vector<WhenClauseContext*> whenClause();
    WhenClauseContext* whenClause(size_t i);
    antlr4::tree::TerminalNode* ELSE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class CurrentLikeContext : public PrimaryExpressionContext {
   public:
    CurrentLikeContext(PrimaryExpressionContext* ctx);

    antlr4::Token* name = nullptr;
    antlr4::tree::TerminalNode* CURRENT_DATE();
    antlr4::tree::TerminalNode* CURRENT_TIMESTAMP();
    antlr4::tree::TerminalNode* CURRENT_USER();
    antlr4::tree::TerminalNode* USER();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ColumnReferenceContext : public PrimaryExpressionContext {
   public:
    ColumnReferenceContext(PrimaryExpressionContext* ctx);

    IdentifierContext* identifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RowConstructorContext : public PrimaryExpressionContext {
   public:
    RowConstructorContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<NamedExpressionContext*> namedExpression();
    NamedExpressionContext* namedExpression(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LastContext : public PrimaryExpressionContext {
   public:
    LastContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* LAST();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* IGNORE();
    antlr4::tree::TerminalNode* NULLS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class StarContext : public PrimaryExpressionContext {
   public:
    StarContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* ASTERISK();
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* DOT();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class OverlayContext : public PrimaryExpressionContext {
   public:
    OverlayContext(PrimaryExpressionContext* ctx);

    SqlBaseParser::ValueExpressionContext* input = nullptr;
    SqlBaseParser::ValueExpressionContext* replace = nullptr;
    SqlBaseParser::ValueExpressionContext* position = nullptr;
    SqlBaseParser::ValueExpressionContext* length = nullptr;
    antlr4::tree::TerminalNode* OVERLAY();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* PLACING();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* FOR();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SubscriptContext : public PrimaryExpressionContext {
   public:
    SubscriptContext(PrimaryExpressionContext* ctx);

    SqlBaseParser::PrimaryExpressionContext* value = nullptr;
    SqlBaseParser::ValueExpressionContext* index = nullptr;
    antlr4::tree::TerminalNode* LEFT_BRACKET();
    antlr4::tree::TerminalNode* RIGHT_BRACKET();
    PrimaryExpressionContext* primaryExpression();
    ValueExpressionContext* valueExpression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TimestampdiffContext : public PrimaryExpressionContext {
   public:
    TimestampdiffContext(PrimaryExpressionContext* ctx);

    antlr4::Token* name = nullptr;
    SqlBaseParser::DatetimeUnitContext* unit = nullptr;
    SqlBaseParser::ValueExpressionContext* startTimestamp = nullptr;
    SqlBaseParser::ValueExpressionContext* endTimestamp = nullptr;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    DatetimeUnitContext* datetimeUnit();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);
    antlr4::tree::TerminalNode* TIMESTAMPDIFF();
    antlr4::tree::TerminalNode* DATEDIFF();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SubqueryExpressionContext : public PrimaryExpressionContext {
   public:
    SubqueryExpressionContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* LEFT_PAREN();
    QueryContext* query();
    antlr4::tree::TerminalNode* RIGHT_PAREN();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ConstantDefaultContext : public PrimaryExpressionContext {
   public:
    ConstantDefaultContext(PrimaryExpressionContext* ctx);

    ConstantContext* constant();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ExtractContext : public PrimaryExpressionContext {
   public:
    ExtractContext(PrimaryExpressionContext* ctx);

    SqlBaseParser::IdentifierContext* field = nullptr;
    SqlBaseParser::ValueExpressionContext* source = nullptr;
    antlr4::tree::TerminalNode* EXTRACT();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    IdentifierContext* identifier();
    ValueExpressionContext* valueExpression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class PercentileContext : public PrimaryExpressionContext {
   public:
    PercentileContext(PrimaryExpressionContext* ctx);

    antlr4::Token* name = nullptr;
    SqlBaseParser::ValueExpressionContext* percentage = nullptr;
    SqlBaseParser::BooleanExpressionContext* where = nullptr;
    std::vector<antlr4::tree::TerminalNode*> LEFT_PAREN();
    antlr4::tree::TerminalNode* LEFT_PAREN(size_t i);
    std::vector<antlr4::tree::TerminalNode*> RIGHT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN(size_t i);
    antlr4::tree::TerminalNode* WITHIN();
    antlr4::tree::TerminalNode* GROUP();
    antlr4::tree::TerminalNode* ORDER();
    antlr4::tree::TerminalNode* BY();
    SortItemContext* sortItem();
    ValueExpressionContext* valueExpression();
    antlr4::tree::TerminalNode* PERCENTILE_CONT();
    antlr4::tree::TerminalNode* PERCENTILE_DISC();
    antlr4::tree::TerminalNode* FILTER();
    antlr4::tree::TerminalNode* WHERE();
    antlr4::tree::TerminalNode* OVER();
    WindowSpecContext* windowSpec();
    BooleanExpressionContext* booleanExpression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class FunctionCallContext : public PrimaryExpressionContext {
   public:
    FunctionCallContext(PrimaryExpressionContext* ctx);

    SqlBaseParser::ExpressionContext* expressionContext = nullptr;
    std::vector<ExpressionContext*> argument;
    SqlBaseParser::BooleanExpressionContext* where = nullptr;
    antlr4::Token* nullsOption = nullptr;
    FunctionNameContext* functionName();
    std::vector<antlr4::tree::TerminalNode*> LEFT_PAREN();
    antlr4::tree::TerminalNode* LEFT_PAREN(size_t i);
    std::vector<antlr4::tree::TerminalNode*> RIGHT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN(size_t i);
    antlr4::tree::TerminalNode* FILTER();
    antlr4::tree::TerminalNode* WHERE();
    antlr4::tree::TerminalNode* NULLS();
    antlr4::tree::TerminalNode* OVER();
    WindowSpecContext* windowSpec();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    BooleanExpressionContext* booleanExpression();
    antlr4::tree::TerminalNode* IGNORE();
    antlr4::tree::TerminalNode* RESPECT();
    SetQuantifierContext* setQuantifier();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SearchedCaseContext : public PrimaryExpressionContext {
   public:
    SearchedCaseContext(PrimaryExpressionContext* ctx);

    SqlBaseParser::ExpressionContext* elseExpression = nullptr;
    antlr4::tree::TerminalNode* CASE();
    antlr4::tree::TerminalNode* END();
    std::vector<WhenClauseContext*> whenClause();
    WhenClauseContext* whenClause(size_t i);
    antlr4::tree::TerminalNode* ELSE();
    ExpressionContext* expression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class PositionContext : public PrimaryExpressionContext {
   public:
    PositionContext(PrimaryExpressionContext* ctx);

    SqlBaseParser::ValueExpressionContext* substr = nullptr;
    SqlBaseParser::ValueExpressionContext* str = nullptr;
    antlr4::tree::TerminalNode* POSITION();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* IN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<ValueExpressionContext*> valueExpression();
    ValueExpressionContext* valueExpression(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class FirstContext : public PrimaryExpressionContext {
   public:
    FirstContext(PrimaryExpressionContext* ctx);

    antlr4::tree::TerminalNode* FIRST();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* IGNORE();
    antlr4::tree::TerminalNode* NULLS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PrimaryExpressionContext* primaryExpression();
  PrimaryExpressionContext* primaryExpression(int precedence);
  class ConstantContext : public antlr4::ParserRuleContext {
   public:
    ConstantContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    ConstantContext() = default;
    void copyFrom(ConstantContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class NullLiteralContext : public ConstantContext {
   public:
    NullLiteralContext(ConstantContext* ctx);

    antlr4::tree::TerminalNode* NULL_LITERAL();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class StringLiteralContext : public ConstantContext {
   public:
    StringLiteralContext(ConstantContext* ctx);

    std::vector<StringLitContext*> stringLit();
    StringLitContext* stringLit(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TypeConstructorContext : public ConstantContext {
   public:
    TypeConstructorContext(ConstantContext* ctx);

    IdentifierContext* identifier();
    StringLitContext* stringLit();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ParameterLiteralContext : public ConstantContext {
   public:
    ParameterLiteralContext(ConstantContext* ctx);

    antlr4::tree::TerminalNode* COLON();
    IdentifierContext* identifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class IntervalLiteralContext : public ConstantContext {
   public:
    IntervalLiteralContext(ConstantContext* ctx);

    IntervalContext* interval();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class NumericLiteralContext : public ConstantContext {
   public:
    NumericLiteralContext(ConstantContext* ctx);

    NumberContext* number();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class BooleanLiteralContext : public ConstantContext {
   public:
    BooleanLiteralContext(ConstantContext* ctx);

    BooleanValueContext* booleanValue();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstantContext* constant();

  class ComparisonOperatorContext : public antlr4::ParserRuleContext {
   public:
    ComparisonOperatorContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* EQ();
    antlr4::tree::TerminalNode* NEQ();
    antlr4::tree::TerminalNode* NEQJ();
    antlr4::tree::TerminalNode* LT();
    antlr4::tree::TerminalNode* LTE();
    antlr4::tree::TerminalNode* GT();
    antlr4::tree::TerminalNode* GTE();
    antlr4::tree::TerminalNode* NSEQ();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ComparisonOperatorContext* comparisonOperator();

  class ArithmeticOperatorContext : public antlr4::ParserRuleContext {
   public:
    ArithmeticOperatorContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* PLUS();
    antlr4::tree::TerminalNode* MINUS();
    antlr4::tree::TerminalNode* ASTERISK();
    antlr4::tree::TerminalNode* SLASH();
    antlr4::tree::TerminalNode* PERCENT();
    antlr4::tree::TerminalNode* DIV();
    antlr4::tree::TerminalNode* TILDE();
    antlr4::tree::TerminalNode* AMPERSAND();
    antlr4::tree::TerminalNode* PIPE();
    antlr4::tree::TerminalNode* CONCAT_PIPE();
    antlr4::tree::TerminalNode* HAT();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ArithmeticOperatorContext* arithmeticOperator();

  class PredicateOperatorContext : public antlr4::ParserRuleContext {
   public:
    PredicateOperatorContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* OR();
    antlr4::tree::TerminalNode* AND();
    antlr4::tree::TerminalNode* IN();
    antlr4::tree::TerminalNode* NOT();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PredicateOperatorContext* predicateOperator();

  class BooleanValueContext : public antlr4::ParserRuleContext {
   public:
    BooleanValueContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* TRUE();
    antlr4::tree::TerminalNode* FALSE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BooleanValueContext* booleanValue();

  class IntervalContext : public antlr4::ParserRuleContext {
   public:
    IntervalContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTERVAL();
    ErrorCapturingMultiUnitsIntervalContext* errorCapturingMultiUnitsInterval();
    ErrorCapturingUnitToUnitIntervalContext* errorCapturingUnitToUnitInterval();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IntervalContext* interval();

  class ErrorCapturingMultiUnitsIntervalContext
      : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::MultiUnitsIntervalContext* body = nullptr;
    ErrorCapturingMultiUnitsIntervalContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MultiUnitsIntervalContext* multiUnitsInterval();
    UnitToUnitIntervalContext* unitToUnitInterval();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ErrorCapturingMultiUnitsIntervalContext* errorCapturingMultiUnitsInterval();

  class MultiUnitsIntervalContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::UnitInMultiUnitsContext* unitInMultiUnitsContext = nullptr;
    std::vector<UnitInMultiUnitsContext*> unit;
    MultiUnitsIntervalContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IntervalValueContext*> intervalValue();
    IntervalValueContext* intervalValue(size_t i);
    std::vector<UnitInMultiUnitsContext*> unitInMultiUnits();
    UnitInMultiUnitsContext* unitInMultiUnits(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultiUnitsIntervalContext* multiUnitsInterval();

  class ErrorCapturingUnitToUnitIntervalContext
      : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::UnitToUnitIntervalContext* body = nullptr;
    SqlBaseParser::MultiUnitsIntervalContext* error1 = nullptr;
    SqlBaseParser::UnitToUnitIntervalContext* error2 = nullptr;
    ErrorCapturingUnitToUnitIntervalContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<UnitToUnitIntervalContext*> unitToUnitInterval();
    UnitToUnitIntervalContext* unitToUnitInterval(size_t i);
    MultiUnitsIntervalContext* multiUnitsInterval();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ErrorCapturingUnitToUnitIntervalContext* errorCapturingUnitToUnitInterval();

  class UnitToUnitIntervalContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::IntervalValueContext* value = nullptr;
    SqlBaseParser::UnitInUnitToUnitContext* from = nullptr;
    SqlBaseParser::UnitInUnitToUnitContext* to = nullptr;
    UnitToUnitIntervalContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* TO();
    IntervalValueContext* intervalValue();
    std::vector<UnitInUnitToUnitContext*> unitInUnitToUnit();
    UnitInUnitToUnitContext* unitInUnitToUnit(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnitToUnitIntervalContext* unitToUnitInterval();

  class IntervalValueContext : public antlr4::ParserRuleContext {
   public:
    IntervalValueContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    antlr4::tree::TerminalNode* DECIMAL_VALUE();
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* PLUS();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IntervalValueContext* intervalValue();

  class UnitInMultiUnitsContext : public antlr4::ParserRuleContext {
   public:
    UnitInMultiUnitsContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NANOSECOND();
    antlr4::tree::TerminalNode* NANOSECONDS();
    antlr4::tree::TerminalNode* MICROSECOND();
    antlr4::tree::TerminalNode* MICROSECONDS();
    antlr4::tree::TerminalNode* MILLISECOND();
    antlr4::tree::TerminalNode* MILLISECONDS();
    antlr4::tree::TerminalNode* SECOND();
    antlr4::tree::TerminalNode* SECONDS();
    antlr4::tree::TerminalNode* MINUTE();
    antlr4::tree::TerminalNode* MINUTES();
    antlr4::tree::TerminalNode* HOUR();
    antlr4::tree::TerminalNode* HOURS();
    antlr4::tree::TerminalNode* DAY();
    antlr4::tree::TerminalNode* DAYS();
    antlr4::tree::TerminalNode* WEEK();
    antlr4::tree::TerminalNode* WEEKS();
    antlr4::tree::TerminalNode* MONTH();
    antlr4::tree::TerminalNode* MONTHS();
    antlr4::tree::TerminalNode* YEAR();
    antlr4::tree::TerminalNode* YEARS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnitInMultiUnitsContext* unitInMultiUnits();

  class UnitInUnitToUnitContext : public antlr4::ParserRuleContext {
   public:
    UnitInUnitToUnitContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SECOND();
    antlr4::tree::TerminalNode* MINUTE();
    antlr4::tree::TerminalNode* HOUR();
    antlr4::tree::TerminalNode* DAY();
    antlr4::tree::TerminalNode* MONTH();
    antlr4::tree::TerminalNode* YEAR();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnitInUnitToUnitContext* unitInUnitToUnit();

  class ColPositionContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* position = nullptr;
    SqlBaseParser::ErrorCapturingIdentifierContext* afterCol = nullptr;
    ColPositionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* FIRST();
    antlr4::tree::TerminalNode* AFTER();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ColPositionContext* colPosition();

  class DataTypeContext : public antlr4::ParserRuleContext {
   public:
    DataTypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    DataTypeContext() = default;
    void copyFrom(DataTypeContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class ComplexDataTypeContext : public DataTypeContext {
   public:
    ComplexDataTypeContext(DataTypeContext* ctx);

    antlr4::Token* complex = nullptr;
    antlr4::tree::TerminalNode* LT();
    std::vector<DataTypeContext*> dataType();
    DataTypeContext* dataType(size_t i);
    antlr4::tree::TerminalNode* GT();
    antlr4::tree::TerminalNode* ARRAY();
    antlr4::tree::TerminalNode* COMMA();
    antlr4::tree::TerminalNode* MAP();
    antlr4::tree::TerminalNode* STRUCT();
    antlr4::tree::TerminalNode* NEQ();
    ComplexColTypeListContext* complexColTypeList();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class YearMonthIntervalDataTypeContext : public DataTypeContext {
   public:
    YearMonthIntervalDataTypeContext(DataTypeContext* ctx);

    antlr4::Token* from = nullptr;
    antlr4::Token* to = nullptr;
    antlr4::tree::TerminalNode* INTERVAL();
    antlr4::tree::TerminalNode* YEAR();
    std::vector<antlr4::tree::TerminalNode*> MONTH();
    antlr4::tree::TerminalNode* MONTH(size_t i);
    antlr4::tree::TerminalNode* TO();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DayTimeIntervalDataTypeContext : public DataTypeContext {
   public:
    DayTimeIntervalDataTypeContext(DataTypeContext* ctx);

    antlr4::Token* from = nullptr;
    antlr4::Token* to = nullptr;
    antlr4::tree::TerminalNode* INTERVAL();
    antlr4::tree::TerminalNode* DAY();
    std::vector<antlr4::tree::TerminalNode*> HOUR();
    antlr4::tree::TerminalNode* HOUR(size_t i);
    std::vector<antlr4::tree::TerminalNode*> MINUTE();
    antlr4::tree::TerminalNode* MINUTE(size_t i);
    std::vector<antlr4::tree::TerminalNode*> SECOND();
    antlr4::tree::TerminalNode* SECOND(size_t i);
    antlr4::tree::TerminalNode* TO();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class PrimitiveDataTypeContext : public DataTypeContext {
   public:
    PrimitiveDataTypeContext(DataTypeContext* ctx);

    IdentifierContext* identifier();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> INTEGER_VALUE();
    antlr4::tree::TerminalNode* INTEGER_VALUE(size_t i);
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DataTypeContext* dataType();

  class QualifiedColTypeWithPositionListContext
      : public antlr4::ParserRuleContext {
   public:
    QualifiedColTypeWithPositionListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<QualifiedColTypeWithPositionContext*>
    qualifiedColTypeWithPosition();
    QualifiedColTypeWithPositionContext* qualifiedColTypeWithPosition(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QualifiedColTypeWithPositionListContext* qualifiedColTypeWithPositionList();

  class QualifiedColTypeWithPositionContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::MultipartIdentifierContext* name = nullptr;
    QualifiedColTypeWithPositionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DataTypeContext* dataType();
    MultipartIdentifierContext* multipartIdentifier();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    DefaultExpressionContext* defaultExpression();
    CommentSpecContext* commentSpec();
    ColPositionContext* colPosition();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QualifiedColTypeWithPositionContext* qualifiedColTypeWithPosition();

  class DefaultExpressionContext : public antlr4::ParserRuleContext {
   public:
    DefaultExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* DEFAULT();
    ExpressionContext* expression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DefaultExpressionContext* defaultExpression();

  class ColTypeListContext : public antlr4::ParserRuleContext {
   public:
    ColTypeListContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ColTypeContext*> colType();
    ColTypeContext* colType(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ColTypeListContext* colTypeList();

  class ColTypeContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ErrorCapturingIdentifierContext* colName = nullptr;
    ColTypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DataTypeContext* dataType();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    CommentSpecContext* commentSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ColTypeContext* colType();

  class CreateOrReplaceTableColTypeListContext
      : public antlr4::ParserRuleContext {
   public:
    CreateOrReplaceTableColTypeListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<CreateOrReplaceTableColTypeContext*>
    createOrReplaceTableColType();
    CreateOrReplaceTableColTypeContext* createOrReplaceTableColType(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CreateOrReplaceTableColTypeListContext* createOrReplaceTableColTypeList();

  class CreateOrReplaceTableColTypeContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ErrorCapturingIdentifierContext* colName = nullptr;
    CreateOrReplaceTableColTypeContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DataTypeContext* dataType();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier();
    std::vector<ColDefinitionOptionContext*> colDefinitionOption();
    ColDefinitionOptionContext* colDefinitionOption(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CreateOrReplaceTableColTypeContext* createOrReplaceTableColType();

  class ColDefinitionOptionContext : public antlr4::ParserRuleContext {
   public:
    ColDefinitionOptionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    DefaultExpressionContext* defaultExpression();
    GenerationExpressionContext* generationExpression();
    CommentSpecContext* commentSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ColDefinitionOptionContext* colDefinitionOption();

  class GenerationExpressionContext : public antlr4::ParserRuleContext {
   public:
    GenerationExpressionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* GENERATED();
    antlr4::tree::TerminalNode* ALWAYS();
    antlr4::tree::TerminalNode* AS();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* RIGHT_PAREN();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GenerationExpressionContext* generationExpression();

  class ComplexColTypeListContext : public antlr4::ParserRuleContext {
   public:
    ComplexColTypeListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ComplexColTypeContext*> complexColType();
    ComplexColTypeContext* complexColType(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ComplexColTypeListContext* complexColTypeList();

  class ComplexColTypeContext : public antlr4::ParserRuleContext {
   public:
    ComplexColTypeContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    DataTypeContext* dataType();
    antlr4::tree::TerminalNode* COLON();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    CommentSpecContext* commentSpec();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ComplexColTypeContext* complexColType();

  class WhenClauseContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ExpressionContext* condition = nullptr;
    SqlBaseParser::ExpressionContext* result = nullptr;
    WhenClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WHEN();
    antlr4::tree::TerminalNode* THEN();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WhenClauseContext* whenClause();

  class WindowClauseContext : public antlr4::ParserRuleContext {
   public:
    WindowClauseContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* WINDOW();
    std::vector<NamedWindowContext*> namedWindow();
    NamedWindowContext* namedWindow(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WindowClauseContext* windowClause();

  class NamedWindowContext : public antlr4::ParserRuleContext {
   public:
    SqlBaseParser::ErrorCapturingIdentifierContext* name = nullptr;
    NamedWindowContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* AS();
    WindowSpecContext* windowSpec();
    ErrorCapturingIdentifierContext* errorCapturingIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NamedWindowContext* namedWindow();

  class WindowSpecContext : public antlr4::ParserRuleContext {
   public:
    WindowSpecContext(antlr4::ParserRuleContext* parent, size_t invokingState);

    WindowSpecContext() = default;
    void copyFrom(WindowSpecContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class WindowRefContext : public WindowSpecContext {
   public:
    WindowRefContext(WindowSpecContext* ctx);

    SqlBaseParser::ErrorCapturingIdentifierContext* name = nullptr;
    ErrorCapturingIdentifierContext* errorCapturingIdentifier();
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class WindowDefContext : public WindowSpecContext {
   public:
    WindowDefContext(WindowSpecContext* ctx);

    SqlBaseParser::ExpressionContext* expressionContext = nullptr;
    std::vector<ExpressionContext*> partition;
    antlr4::tree::TerminalNode* LEFT_PAREN();
    antlr4::tree::TerminalNode* RIGHT_PAREN();
    antlr4::tree::TerminalNode* CLUSTER();
    std::vector<antlr4::tree::TerminalNode*> BY();
    antlr4::tree::TerminalNode* BY(size_t i);
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    WindowFrameContext* windowFrame();
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    std::vector<SortItemContext*> sortItem();
    SortItemContext* sortItem(size_t i);
    antlr4::tree::TerminalNode* PARTITION();
    antlr4::tree::TerminalNode* DISTRIBUTE();
    antlr4::tree::TerminalNode* ORDER();
    antlr4::tree::TerminalNode* SORT();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WindowSpecContext* windowSpec();

  class WindowFrameContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* frameType = nullptr;
    SqlBaseParser::FrameBoundContext* start = nullptr;
    SqlBaseParser::FrameBoundContext* end = nullptr;
    WindowFrameContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* RANGE();
    std::vector<FrameBoundContext*> frameBound();
    FrameBoundContext* frameBound(size_t i);
    antlr4::tree::TerminalNode* ROWS();
    antlr4::tree::TerminalNode* BETWEEN();
    antlr4::tree::TerminalNode* AND();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WindowFrameContext* windowFrame();

  class FrameBoundContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* boundType = nullptr;
    FrameBoundContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* UNBOUNDED();
    antlr4::tree::TerminalNode* PRECEDING();
    antlr4::tree::TerminalNode* FOLLOWING();
    antlr4::tree::TerminalNode* ROW();
    antlr4::tree::TerminalNode* CURRENT();
    ExpressionContext* expression();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FrameBoundContext* frameBound();

  class QualifiedNameListContext : public antlr4::ParserRuleContext {
   public:
    QualifiedNameListContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<QualifiedNameContext*> qualifiedName();
    QualifiedNameContext* qualifiedName(size_t i);
    std::vector<antlr4::tree::TerminalNode*> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QualifiedNameListContext* qualifiedNameList();

  class FunctionNameContext : public antlr4::ParserRuleContext {
   public:
    FunctionNameContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QualifiedNameContext* qualifiedName();
    antlr4::tree::TerminalNode* FILTER();
    antlr4::tree::TerminalNode* LEFT();
    antlr4::tree::TerminalNode* RIGHT();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FunctionNameContext* functionName();

  class QualifiedNameContext : public antlr4::ParserRuleContext {
   public:
    QualifiedNameContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);
    std::vector<antlr4::tree::TerminalNode*> DOT();
    antlr4::tree::TerminalNode* DOT(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QualifiedNameContext* qualifiedName();

  class ErrorCapturingIdentifierContext : public antlr4::ParserRuleContext {
   public:
    ErrorCapturingIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdentifierContext* identifier();
    ErrorCapturingIdentifierExtraContext* errorCapturingIdentifierExtra();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ErrorCapturingIdentifierContext* errorCapturingIdentifier();

  class ErrorCapturingIdentifierExtraContext
      : public antlr4::ParserRuleContext {
   public:
    ErrorCapturingIdentifierExtraContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    ErrorCapturingIdentifierExtraContext() = default;
    void copyFrom(ErrorCapturingIdentifierExtraContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class ErrorIdentContext : public ErrorCapturingIdentifierExtraContext {
   public:
    ErrorIdentContext(ErrorCapturingIdentifierExtraContext* ctx);

    std::vector<antlr4::tree::TerminalNode*> MINUS();
    antlr4::tree::TerminalNode* MINUS(size_t i);
    std::vector<IdentifierContext*> identifier();
    IdentifierContext* identifier(size_t i);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class RealIdentContext : public ErrorCapturingIdentifierExtraContext {
   public:
    RealIdentContext(ErrorCapturingIdentifierExtraContext* ctx);

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ErrorCapturingIdentifierExtraContext* errorCapturingIdentifierExtra();

  class IdentifierContext : public antlr4::ParserRuleContext {
   public:
    IdentifierContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StrictIdentifierContext* strictIdentifier();
    StrictNonReservedContext* strictNonReserved();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IdentifierContext* identifier();

  class StrictIdentifierContext : public antlr4::ParserRuleContext {
   public:
    StrictIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);

    StrictIdentifierContext() = default;
    void copyFrom(StrictIdentifierContext* context);
    using antlr4::ParserRuleContext::copyFrom;

    virtual size_t getRuleIndex() const override;
  };

  class QuotedIdentifierAlternativeContext : public StrictIdentifierContext {
   public:
    QuotedIdentifierAlternativeContext(StrictIdentifierContext* ctx);

    QuotedIdentifierContext* quotedIdentifier();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class UnquotedIdentifierContext : public StrictIdentifierContext {
   public:
    UnquotedIdentifierContext(StrictIdentifierContext* ctx);

    antlr4::tree::TerminalNode* IDENTIFIER();
    AnsiNonReservedContext* ansiNonReserved();
    NonReservedContext* nonReserved();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StrictIdentifierContext* strictIdentifier();

  class QuotedIdentifierContext : public antlr4::ParserRuleContext {
   public:
    QuotedIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* BACKQUOTED_IDENTIFIER();
    antlr4::tree::TerminalNode* DOUBLEQUOTED_STRING();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QuotedIdentifierContext* quotedIdentifier();

  class BackQuotedIdentifierContext : public antlr4::ParserRuleContext {
   public:
    BackQuotedIdentifierContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* BACKQUOTED_IDENTIFIER();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BackQuotedIdentifierContext* backQuotedIdentifier();

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
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class BigIntLiteralContext : public NumberContext {
   public:
    BigIntLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* BIGINT_LITERAL();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class TinyIntLiteralContext : public NumberContext {
   public:
    TinyIntLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* TINYINT_LITERAL();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class LegacyDecimalLiteralContext : public NumberContext {
   public:
    LegacyDecimalLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* EXPONENT_VALUE();
    antlr4::tree::TerminalNode* DECIMAL_VALUE();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class BigDecimalLiteralContext : public NumberContext {
   public:
    BigDecimalLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* BIGDECIMAL_LITERAL();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class ExponentLiteralContext : public NumberContext {
   public:
    ExponentLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* EXPONENT_VALUE();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class DoubleLiteralContext : public NumberContext {
   public:
    DoubleLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* DOUBLE_LITERAL();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class IntegerLiteralContext : public NumberContext {
   public:
    IntegerLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* INTEGER_VALUE();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class FloatLiteralContext : public NumberContext {
   public:
    FloatLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* FLOAT_LITERAL();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  class SmallIntLiteralContext : public NumberContext {
   public:
    SmallIntLiteralContext(NumberContext* ctx);

    antlr4::tree::TerminalNode* SMALLINT_LITERAL();
    antlr4::tree::TerminalNode* MINUS();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NumberContext* number();

  class AlterColumnActionContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* setOrDrop = nullptr;
    antlr4::Token* dropDefault = nullptr;
    AlterColumnActionContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* TYPE();
    DataTypeContext* dataType();
    CommentSpecContext* commentSpec();
    ColPositionContext* colPosition();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* DROP();
    DefaultExpressionContext* defaultExpression();
    antlr4::tree::TerminalNode* DEFAULT();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AlterColumnActionContext* alterColumnAction();

  class StringLitContext : public antlr4::ParserRuleContext {
   public:
    StringLitContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* STRING();
    antlr4::tree::TerminalNode* DOUBLEQUOTED_STRING();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StringLitContext* stringLit();

  class CommentContext : public antlr4::ParserRuleContext {
   public:
    CommentContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StringLitContext* stringLit();
    antlr4::tree::TerminalNode* NULL_LITERAL();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CommentContext* comment();

  class VersionContext : public antlr4::ParserRuleContext {
   public:
    VersionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTEGER_VALUE();
    StringLitContext* stringLit();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VersionContext* version();

  class AnsiNonReservedContext : public antlr4::ParserRuleContext {
   public:
    AnsiNonReservedContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ADD();
    antlr4::tree::TerminalNode* AFTER();
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* ALWAYS();
    antlr4::tree::TerminalNode* ANALYZE();
    antlr4::tree::TerminalNode* ANTI();
    antlr4::tree::TerminalNode* ANY_VALUE();
    antlr4::tree::TerminalNode* ARCHIVE();
    antlr4::tree::TerminalNode* ARRAY();
    antlr4::tree::TerminalNode* ASC();
    antlr4::tree::TerminalNode* AT();
    antlr4::tree::TerminalNode* BETWEEN();
    antlr4::tree::TerminalNode* BUCKET();
    antlr4::tree::TerminalNode* BUCKETS();
    antlr4::tree::TerminalNode* BY();
    antlr4::tree::TerminalNode* CACHE();
    antlr4::tree::TerminalNode* CASCADE();
    antlr4::tree::TerminalNode* CATALOG();
    antlr4::tree::TerminalNode* CATALOGS();
    antlr4::tree::TerminalNode* CHANGE();
    antlr4::tree::TerminalNode* CLEAR();
    antlr4::tree::TerminalNode* CLUSTER();
    antlr4::tree::TerminalNode* CLUSTERED();
    antlr4::tree::TerminalNode* CODEGEN();
    antlr4::tree::TerminalNode* COLLECTION();
    antlr4::tree::TerminalNode* COLUMNS();
    antlr4::tree::TerminalNode* COMMENT();
    antlr4::tree::TerminalNode* COMMIT();
    antlr4::tree::TerminalNode* COMPACT();
    antlr4::tree::TerminalNode* COMPACTIONS();
    antlr4::tree::TerminalNode* COMPUTE();
    antlr4::tree::TerminalNode* CONCATENATE();
    antlr4::tree::TerminalNode* COST();
    antlr4::tree::TerminalNode* CUBE();
    antlr4::tree::TerminalNode* CURRENT();
    antlr4::tree::TerminalNode* DATA();
    antlr4::tree::TerminalNode* DATABASE();
    antlr4::tree::TerminalNode* DATABASES();
    antlr4::tree::TerminalNode* DATEADD();
    antlr4::tree::TerminalNode* DATEDIFF();
    antlr4::tree::TerminalNode* DAY();
    antlr4::tree::TerminalNode* DAYS();
    antlr4::tree::TerminalNode* DAYOFYEAR();
    antlr4::tree::TerminalNode* DBPROPERTIES();
    antlr4::tree::TerminalNode* DEFAULT();
    antlr4::tree::TerminalNode* DEFINED();
    antlr4::tree::TerminalNode* DELETE();
    antlr4::tree::TerminalNode* DELIMITED();
    antlr4::tree::TerminalNode* DESC();
    antlr4::tree::TerminalNode* DESCRIBE();
    antlr4::tree::TerminalNode* DFS();
    antlr4::tree::TerminalNode* DIRECTORIES();
    antlr4::tree::TerminalNode* DIRECTORY();
    antlr4::tree::TerminalNode* DISTRIBUTE();
    antlr4::tree::TerminalNode* DIV();
    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* ESCAPED();
    antlr4::tree::TerminalNode* EXCHANGE();
    antlr4::tree::TerminalNode* EXCLUDE();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* EXPLAIN();
    antlr4::tree::TerminalNode* EXPORT();
    antlr4::tree::TerminalNode* EXTENDED();
    antlr4::tree::TerminalNode* EXTERNAL();
    antlr4::tree::TerminalNode* EXTRACT();
    antlr4::tree::TerminalNode* FIELDS();
    antlr4::tree::TerminalNode* FILEFORMAT();
    antlr4::tree::TerminalNode* FIRST();
    antlr4::tree::TerminalNode* FOLLOWING();
    antlr4::tree::TerminalNode* FORMAT();
    antlr4::tree::TerminalNode* FORMATTED();
    antlr4::tree::TerminalNode* FUNCTION();
    antlr4::tree::TerminalNode* FUNCTIONS();
    antlr4::tree::TerminalNode* GENERATED();
    antlr4::tree::TerminalNode* GLOBAL();
    antlr4::tree::TerminalNode* GROUPING();
    antlr4::tree::TerminalNode* HOUR();
    antlr4::tree::TerminalNode* HOURS();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* IGNORE();
    antlr4::tree::TerminalNode* IMPORT();
    antlr4::tree::TerminalNode* INCLUDE();
    antlr4::tree::TerminalNode* INDEX();
    antlr4::tree::TerminalNode* INDEXES();
    antlr4::tree::TerminalNode* INPATH();
    antlr4::tree::TerminalNode* INPUTFORMAT();
    antlr4::tree::TerminalNode* INSERT();
    antlr4::tree::TerminalNode* INTERVAL();
    antlr4::tree::TerminalNode* ITEMS();
    antlr4::tree::TerminalNode* KEYS();
    antlr4::tree::TerminalNode* LAST();
    antlr4::tree::TerminalNode* LAZY();
    antlr4::tree::TerminalNode* LIKE();
    antlr4::tree::TerminalNode* ILIKE();
    antlr4::tree::TerminalNode* LIMIT();
    antlr4::tree::TerminalNode* LINES();
    antlr4::tree::TerminalNode* LIST();
    antlr4::tree::TerminalNode* LOAD();
    antlr4::tree::TerminalNode* LOCAL();
    antlr4::tree::TerminalNode* LOCATION();
    antlr4::tree::TerminalNode* LOCK();
    antlr4::tree::TerminalNode* LOCKS();
    antlr4::tree::TerminalNode* LOGICAL();
    antlr4::tree::TerminalNode* MACRO();
    antlr4::tree::TerminalNode* MAP();
    antlr4::tree::TerminalNode* MATCHED();
    antlr4::tree::TerminalNode* MERGE();
    antlr4::tree::TerminalNode* MICROSECOND();
    antlr4::tree::TerminalNode* MICROSECONDS();
    antlr4::tree::TerminalNode* MILLISECOND();
    antlr4::tree::TerminalNode* MILLISECONDS();
    antlr4::tree::TerminalNode* MINUTE();
    antlr4::tree::TerminalNode* MINUTES();
    antlr4::tree::TerminalNode* MONTH();
    antlr4::tree::TerminalNode* MONTHS();
    antlr4::tree::TerminalNode* MSCK();
    antlr4::tree::TerminalNode* NAMESPACE();
    antlr4::tree::TerminalNode* NAMESPACES();
    antlr4::tree::TerminalNode* NANOSECOND();
    antlr4::tree::TerminalNode* NANOSECONDS();
    antlr4::tree::TerminalNode* NO();
    antlr4::tree::TerminalNode* NULLS();
    antlr4::tree::TerminalNode* OF();
    antlr4::tree::TerminalNode* OPTION();
    antlr4::tree::TerminalNode* OPTIONS();
    antlr4::tree::TerminalNode* OUT();
    antlr4::tree::TerminalNode* OUTPUTFORMAT();
    antlr4::tree::TerminalNode* OVER();
    antlr4::tree::TerminalNode* OVERLAY();
    antlr4::tree::TerminalNode* OVERWRITE();
    antlr4::tree::TerminalNode* PARTITION();
    antlr4::tree::TerminalNode* PARTITIONED();
    antlr4::tree::TerminalNode* PARTITIONS();
    antlr4::tree::TerminalNode* PERCENTLIT();
    antlr4::tree::TerminalNode* PIVOT();
    antlr4::tree::TerminalNode* PLACING();
    antlr4::tree::TerminalNode* POSITION();
    antlr4::tree::TerminalNode* PRECEDING();
    antlr4::tree::TerminalNode* PRINCIPALS();
    antlr4::tree::TerminalNode* PROPERTIES();
    antlr4::tree::TerminalNode* PURGE();
    antlr4::tree::TerminalNode* QUARTER();
    antlr4::tree::TerminalNode* QUERY();
    antlr4::tree::TerminalNode* RANGE();
    antlr4::tree::TerminalNode* RECORDREADER();
    antlr4::tree::TerminalNode* RECORDWRITER();
    antlr4::tree::TerminalNode* RECOVER();
    antlr4::tree::TerminalNode* REDUCE();
    antlr4::tree::TerminalNode* REFRESH();
    antlr4::tree::TerminalNode* RENAME();
    antlr4::tree::TerminalNode* REPAIR();
    antlr4::tree::TerminalNode* REPEATABLE();
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* RESET();
    antlr4::tree::TerminalNode* RESPECT();
    antlr4::tree::TerminalNode* RESTRICT();
    antlr4::tree::TerminalNode* REVOKE();
    antlr4::tree::TerminalNode* RLIKE();
    antlr4::tree::TerminalNode* ROLE();
    antlr4::tree::TerminalNode* ROLES();
    antlr4::tree::TerminalNode* ROLLBACK();
    antlr4::tree::TerminalNode* ROLLUP();
    antlr4::tree::TerminalNode* ROW();
    antlr4::tree::TerminalNode* ROWS();
    antlr4::tree::TerminalNode* SCHEMA();
    antlr4::tree::TerminalNode* SCHEMAS();
    antlr4::tree::TerminalNode* SECOND();
    antlr4::tree::TerminalNode* SECONDS();
    antlr4::tree::TerminalNode* SEMI();
    antlr4::tree::TerminalNode* SEPARATED();
    antlr4::tree::TerminalNode* SERDE();
    antlr4::tree::TerminalNode* SERDEPROPERTIES();
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* SETMINUS();
    antlr4::tree::TerminalNode* SETS();
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* SKEWED();
    antlr4::tree::TerminalNode* SORT();
    antlr4::tree::TerminalNode* SORTED();
    antlr4::tree::TerminalNode* SOURCE();
    antlr4::tree::TerminalNode* START();
    antlr4::tree::TerminalNode* STATISTICS();
    antlr4::tree::TerminalNode* STORED();
    antlr4::tree::TerminalNode* STRATIFY();
    antlr4::tree::TerminalNode* STRUCT();
    antlr4::tree::TerminalNode* SUBSTR();
    antlr4::tree::TerminalNode* SUBSTRING();
    antlr4::tree::TerminalNode* SYNC();
    antlr4::tree::TerminalNode* SYSTEM_TIME();
    antlr4::tree::TerminalNode* SYSTEM_VERSION();
    antlr4::tree::TerminalNode* TABLES();
    antlr4::tree::TerminalNode* TABLESAMPLE();
    antlr4::tree::TerminalNode* TARGET();
    antlr4::tree::TerminalNode* TBLPROPERTIES();
    antlr4::tree::TerminalNode* TEMPORARY();
    antlr4::tree::TerminalNode* TERMINATED();
    antlr4::tree::TerminalNode* TIMESTAMP();
    antlr4::tree::TerminalNode* TIMESTAMPADD();
    antlr4::tree::TerminalNode* TIMESTAMPDIFF();
    antlr4::tree::TerminalNode* TOUCH();
    antlr4::tree::TerminalNode* TRANSACTION();
    antlr4::tree::TerminalNode* TRANSACTIONS();
    antlr4::tree::TerminalNode* TRANSFORM();
    antlr4::tree::TerminalNode* TRIM();
    antlr4::tree::TerminalNode* TRUE();
    antlr4::tree::TerminalNode* TRUNCATE();
    antlr4::tree::TerminalNode* TRY_CAST();
    antlr4::tree::TerminalNode* TYPE();
    antlr4::tree::TerminalNode* UNARCHIVE();
    antlr4::tree::TerminalNode* UNBOUNDED();
    antlr4::tree::TerminalNode* UNCACHE();
    antlr4::tree::TerminalNode* UNLOCK();
    antlr4::tree::TerminalNode* UNPIVOT();
    antlr4::tree::TerminalNode* UNSET();
    antlr4::tree::TerminalNode* UPDATE();
    antlr4::tree::TerminalNode* USE();
    antlr4::tree::TerminalNode* VALUES();
    antlr4::tree::TerminalNode* VERSION();
    antlr4::tree::TerminalNode* VIEW();
    antlr4::tree::TerminalNode* VIEWS();
    antlr4::tree::TerminalNode* WEEK();
    antlr4::tree::TerminalNode* WEEKS();
    antlr4::tree::TerminalNode* WINDOW();
    antlr4::tree::TerminalNode* YEAR();
    antlr4::tree::TerminalNode* YEARS();
    antlr4::tree::TerminalNode* ZONE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AnsiNonReservedContext* ansiNonReserved();

  class StrictNonReservedContext : public antlr4::ParserRuleContext {
   public:
    StrictNonReservedContext(
        antlr4::ParserRuleContext* parent,
        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ANTI();
    antlr4::tree::TerminalNode* CROSS();
    antlr4::tree::TerminalNode* EXCEPT();
    antlr4::tree::TerminalNode* FULL();
    antlr4::tree::TerminalNode* INNER();
    antlr4::tree::TerminalNode* INTERSECT();
    antlr4::tree::TerminalNode* JOIN();
    antlr4::tree::TerminalNode* LATERAL();
    antlr4::tree::TerminalNode* LEFT();
    antlr4::tree::TerminalNode* NATURAL();
    antlr4::tree::TerminalNode* ON();
    antlr4::tree::TerminalNode* RIGHT();
    antlr4::tree::TerminalNode* SEMI();
    antlr4::tree::TerminalNode* SETMINUS();
    antlr4::tree::TerminalNode* UNION();
    antlr4::tree::TerminalNode* USING();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StrictNonReservedContext* strictNonReserved();

  class NonReservedContext : public antlr4::ParserRuleContext {
   public:
    NonReservedContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ADD();
    antlr4::tree::TerminalNode* AFTER();
    antlr4::tree::TerminalNode* ALL();
    antlr4::tree::TerminalNode* ALTER();
    antlr4::tree::TerminalNode* ALWAYS();
    antlr4::tree::TerminalNode* ANALYZE();
    antlr4::tree::TerminalNode* AND();
    antlr4::tree::TerminalNode* ANY();
    antlr4::tree::TerminalNode* ANY_VALUE();
    antlr4::tree::TerminalNode* ARCHIVE();
    antlr4::tree::TerminalNode* ARRAY();
    antlr4::tree::TerminalNode* AS();
    antlr4::tree::TerminalNode* ASC();
    antlr4::tree::TerminalNode* AT();
    antlr4::tree::TerminalNode* AUTHORIZATION();
    antlr4::tree::TerminalNode* BETWEEN();
    antlr4::tree::TerminalNode* BOTH();
    antlr4::tree::TerminalNode* BUCKET();
    antlr4::tree::TerminalNode* BUCKETS();
    antlr4::tree::TerminalNode* BY();
    antlr4::tree::TerminalNode* CACHE();
    antlr4::tree::TerminalNode* CASCADE();
    antlr4::tree::TerminalNode* CASE();
    antlr4::tree::TerminalNode* CAST();
    antlr4::tree::TerminalNode* CATALOG();
    antlr4::tree::TerminalNode* CATALOGS();
    antlr4::tree::TerminalNode* CHANGE();
    antlr4::tree::TerminalNode* CHECK();
    antlr4::tree::TerminalNode* CLEAR();
    antlr4::tree::TerminalNode* CLUSTER();
    antlr4::tree::TerminalNode* CLUSTERED();
    antlr4::tree::TerminalNode* CODEGEN();
    antlr4::tree::TerminalNode* COLLATE();
    antlr4::tree::TerminalNode* COLLECTION();
    antlr4::tree::TerminalNode* COLUMN();
    antlr4::tree::TerminalNode* COLUMNS();
    antlr4::tree::TerminalNode* COMMENT();
    antlr4::tree::TerminalNode* COMMIT();
    antlr4::tree::TerminalNode* COMPACT();
    antlr4::tree::TerminalNode* COMPACTIONS();
    antlr4::tree::TerminalNode* COMPUTE();
    antlr4::tree::TerminalNode* CONCATENATE();
    antlr4::tree::TerminalNode* CONSTRAINT();
    antlr4::tree::TerminalNode* COST();
    antlr4::tree::TerminalNode* CREATE();
    antlr4::tree::TerminalNode* CUBE();
    antlr4::tree::TerminalNode* CURRENT();
    antlr4::tree::TerminalNode* CURRENT_DATE();
    antlr4::tree::TerminalNode* CURRENT_TIME();
    antlr4::tree::TerminalNode* CURRENT_TIMESTAMP();
    antlr4::tree::TerminalNode* CURRENT_USER();
    antlr4::tree::TerminalNode* DATA();
    antlr4::tree::TerminalNode* DATABASE();
    antlr4::tree::TerminalNode* DATABASES();
    antlr4::tree::TerminalNode* DATEADD();
    antlr4::tree::TerminalNode* DATEDIFF();
    antlr4::tree::TerminalNode* DAY();
    antlr4::tree::TerminalNode* DAYS();
    antlr4::tree::TerminalNode* DAYOFYEAR();
    antlr4::tree::TerminalNode* DBPROPERTIES();
    antlr4::tree::TerminalNode* DEFAULT();
    antlr4::tree::TerminalNode* DEFINED();
    antlr4::tree::TerminalNode* DELETE();
    antlr4::tree::TerminalNode* DELIMITED();
    antlr4::tree::TerminalNode* DESC();
    antlr4::tree::TerminalNode* DESCRIBE();
    antlr4::tree::TerminalNode* DFS();
    antlr4::tree::TerminalNode* DIRECTORIES();
    antlr4::tree::TerminalNode* DIRECTORY();
    antlr4::tree::TerminalNode* DISTINCT();
    antlr4::tree::TerminalNode* DISTRIBUTE();
    antlr4::tree::TerminalNode* DIV();
    antlr4::tree::TerminalNode* DROP();
    antlr4::tree::TerminalNode* ELSE();
    antlr4::tree::TerminalNode* END();
    antlr4::tree::TerminalNode* ESCAPE();
    antlr4::tree::TerminalNode* ESCAPED();
    antlr4::tree::TerminalNode* EXCHANGE();
    antlr4::tree::TerminalNode* EXCLUDE();
    antlr4::tree::TerminalNode* EXISTS();
    antlr4::tree::TerminalNode* EXPLAIN();
    antlr4::tree::TerminalNode* EXPORT();
    antlr4::tree::TerminalNode* EXTENDED();
    antlr4::tree::TerminalNode* EXTERNAL();
    antlr4::tree::TerminalNode* EXTRACT();
    antlr4::tree::TerminalNode* FALSE();
    antlr4::tree::TerminalNode* FETCH();
    antlr4::tree::TerminalNode* FILTER();
    antlr4::tree::TerminalNode* FIELDS();
    antlr4::tree::TerminalNode* FILEFORMAT();
    antlr4::tree::TerminalNode* FIRST();
    antlr4::tree::TerminalNode* FOLLOWING();
    antlr4::tree::TerminalNode* FOR();
    antlr4::tree::TerminalNode* FOREIGN();
    antlr4::tree::TerminalNode* FORMAT();
    antlr4::tree::TerminalNode* FORMATTED();
    antlr4::tree::TerminalNode* FROM();
    antlr4::tree::TerminalNode* FUNCTION();
    antlr4::tree::TerminalNode* FUNCTIONS();
    antlr4::tree::TerminalNode* GENERATED();
    antlr4::tree::TerminalNode* GLOBAL();
    antlr4::tree::TerminalNode* GRANT();
    antlr4::tree::TerminalNode* GROUP();
    antlr4::tree::TerminalNode* GROUPING();
    antlr4::tree::TerminalNode* HAVING();
    antlr4::tree::TerminalNode* HOUR();
    antlr4::tree::TerminalNode* HOURS();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* IGNORE();
    antlr4::tree::TerminalNode* IMPORT();
    antlr4::tree::TerminalNode* IN();
    antlr4::tree::TerminalNode* INCLUDE();
    antlr4::tree::TerminalNode* INDEX();
    antlr4::tree::TerminalNode* INDEXES();
    antlr4::tree::TerminalNode* INPATH();
    antlr4::tree::TerminalNode* INPUTFORMAT();
    antlr4::tree::TerminalNode* INSERT();
    antlr4::tree::TerminalNode* INTERVAL();
    antlr4::tree::TerminalNode* INTO();
    antlr4::tree::TerminalNode* IS();
    antlr4::tree::TerminalNode* ITEMS();
    antlr4::tree::TerminalNode* KEYS();
    antlr4::tree::TerminalNode* LAST();
    antlr4::tree::TerminalNode* LAZY();
    antlr4::tree::TerminalNode* LEADING();
    antlr4::tree::TerminalNode* LIKE();
    antlr4::tree::TerminalNode* ILIKE();
    antlr4::tree::TerminalNode* LIMIT();
    antlr4::tree::TerminalNode* LINES();
    antlr4::tree::TerminalNode* LIST();
    antlr4::tree::TerminalNode* LOAD();
    antlr4::tree::TerminalNode* LOCAL();
    antlr4::tree::TerminalNode* LOCATION();
    antlr4::tree::TerminalNode* LOCK();
    antlr4::tree::TerminalNode* LOCKS();
    antlr4::tree::TerminalNode* LOGICAL();
    antlr4::tree::TerminalNode* MACRO();
    antlr4::tree::TerminalNode* MAP();
    antlr4::tree::TerminalNode* MATCHED();
    antlr4::tree::TerminalNode* MERGE();
    antlr4::tree::TerminalNode* MICROSECOND();
    antlr4::tree::TerminalNode* MICROSECONDS();
    antlr4::tree::TerminalNode* MILLISECOND();
    antlr4::tree::TerminalNode* MILLISECONDS();
    antlr4::tree::TerminalNode* MINUTE();
    antlr4::tree::TerminalNode* MINUTES();
    antlr4::tree::TerminalNode* MONTH();
    antlr4::tree::TerminalNode* MONTHS();
    antlr4::tree::TerminalNode* MSCK();
    antlr4::tree::TerminalNode* NAMESPACE();
    antlr4::tree::TerminalNode* NAMESPACES();
    antlr4::tree::TerminalNode* NANOSECOND();
    antlr4::tree::TerminalNode* NANOSECONDS();
    antlr4::tree::TerminalNode* NO();
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* NULL_LITERAL();
    antlr4::tree::TerminalNode* NULLS();
    antlr4::tree::TerminalNode* OF();
    antlr4::tree::TerminalNode* OFFSET();
    antlr4::tree::TerminalNode* ONLY();
    antlr4::tree::TerminalNode* OPTION();
    antlr4::tree::TerminalNode* OPTIONS();
    antlr4::tree::TerminalNode* OR();
    antlr4::tree::TerminalNode* ORDER();
    antlr4::tree::TerminalNode* OUT();
    antlr4::tree::TerminalNode* OUTER();
    antlr4::tree::TerminalNode* OUTPUTFORMAT();
    antlr4::tree::TerminalNode* OVER();
    antlr4::tree::TerminalNode* OVERLAPS();
    antlr4::tree::TerminalNode* OVERLAY();
    antlr4::tree::TerminalNode* OVERWRITE();
    antlr4::tree::TerminalNode* PARTITION();
    antlr4::tree::TerminalNode* PARTITIONED();
    antlr4::tree::TerminalNode* PARTITIONS();
    antlr4::tree::TerminalNode* PERCENTILE_CONT();
    antlr4::tree::TerminalNode* PERCENTILE_DISC();
    antlr4::tree::TerminalNode* PERCENTLIT();
    antlr4::tree::TerminalNode* PIVOT();
    antlr4::tree::TerminalNode* PLACING();
    antlr4::tree::TerminalNode* POSITION();
    antlr4::tree::TerminalNode* PRECEDING();
    antlr4::tree::TerminalNode* PRIMARY();
    antlr4::tree::TerminalNode* PRINCIPALS();
    antlr4::tree::TerminalNode* PROPERTIES();
    antlr4::tree::TerminalNode* PURGE();
    antlr4::tree::TerminalNode* QUARTER();
    antlr4::tree::TerminalNode* QUERY();
    antlr4::tree::TerminalNode* RANGE();
    antlr4::tree::TerminalNode* RECORDREADER();
    antlr4::tree::TerminalNode* RECORDWRITER();
    antlr4::tree::TerminalNode* RECOVER();
    antlr4::tree::TerminalNode* REDUCE();
    antlr4::tree::TerminalNode* REFERENCES();
    antlr4::tree::TerminalNode* REFRESH();
    antlr4::tree::TerminalNode* RENAME();
    antlr4::tree::TerminalNode* REPAIR();
    antlr4::tree::TerminalNode* REPEATABLE();
    antlr4::tree::TerminalNode* REPLACE();
    antlr4::tree::TerminalNode* RESET();
    antlr4::tree::TerminalNode* RESPECT();
    antlr4::tree::TerminalNode* RESTRICT();
    antlr4::tree::TerminalNode* REVOKE();
    antlr4::tree::TerminalNode* RLIKE();
    antlr4::tree::TerminalNode* ROLE();
    antlr4::tree::TerminalNode* ROLES();
    antlr4::tree::TerminalNode* ROLLBACK();
    antlr4::tree::TerminalNode* ROLLUP();
    antlr4::tree::TerminalNode* ROW();
    antlr4::tree::TerminalNode* ROWS();
    antlr4::tree::TerminalNode* SCHEMA();
    antlr4::tree::TerminalNode* SCHEMAS();
    antlr4::tree::TerminalNode* SECOND();
    antlr4::tree::TerminalNode* SECONDS();
    antlr4::tree::TerminalNode* SELECT();
    antlr4::tree::TerminalNode* SEPARATED();
    antlr4::tree::TerminalNode* SERDE();
    antlr4::tree::TerminalNode* SERDEPROPERTIES();
    antlr4::tree::TerminalNode* SESSION_USER();
    antlr4::tree::TerminalNode* SET();
    antlr4::tree::TerminalNode* SETS();
    antlr4::tree::TerminalNode* SHOW();
    antlr4::tree::TerminalNode* SKEWED();
    antlr4::tree::TerminalNode* SOME();
    antlr4::tree::TerminalNode* SORT();
    antlr4::tree::TerminalNode* SORTED();
    antlr4::tree::TerminalNode* SOURCE();
    antlr4::tree::TerminalNode* START();
    antlr4::tree::TerminalNode* STATISTICS();
    antlr4::tree::TerminalNode* STORED();
    antlr4::tree::TerminalNode* STRATIFY();
    antlr4::tree::TerminalNode* STRUCT();
    antlr4::tree::TerminalNode* SUBSTR();
    antlr4::tree::TerminalNode* SUBSTRING();
    antlr4::tree::TerminalNode* SYNC();
    antlr4::tree::TerminalNode* SYSTEM_TIME();
    antlr4::tree::TerminalNode* SYSTEM_VERSION();
    antlr4::tree::TerminalNode* TABLE();
    antlr4::tree::TerminalNode* TABLES();
    antlr4::tree::TerminalNode* TABLESAMPLE();
    antlr4::tree::TerminalNode* TARGET();
    antlr4::tree::TerminalNode* TBLPROPERTIES();
    antlr4::tree::TerminalNode* TEMPORARY();
    antlr4::tree::TerminalNode* TERMINATED();
    antlr4::tree::TerminalNode* THEN();
    antlr4::tree::TerminalNode* TIME();
    antlr4::tree::TerminalNode* TIMESTAMP();
    antlr4::tree::TerminalNode* TIMESTAMPADD();
    antlr4::tree::TerminalNode* TIMESTAMPDIFF();
    antlr4::tree::TerminalNode* TO();
    antlr4::tree::TerminalNode* TOUCH();
    antlr4::tree::TerminalNode* TRAILING();
    antlr4::tree::TerminalNode* TRANSACTION();
    antlr4::tree::TerminalNode* TRANSACTIONS();
    antlr4::tree::TerminalNode* TRANSFORM();
    antlr4::tree::TerminalNode* TRIM();
    antlr4::tree::TerminalNode* TRUE();
    antlr4::tree::TerminalNode* TRUNCATE();
    antlr4::tree::TerminalNode* TRY_CAST();
    antlr4::tree::TerminalNode* TYPE();
    antlr4::tree::TerminalNode* UNARCHIVE();
    antlr4::tree::TerminalNode* UNBOUNDED();
    antlr4::tree::TerminalNode* UNCACHE();
    antlr4::tree::TerminalNode* UNIQUE();
    antlr4::tree::TerminalNode* UNKNOWN();
    antlr4::tree::TerminalNode* UNLOCK();
    antlr4::tree::TerminalNode* UNPIVOT();
    antlr4::tree::TerminalNode* UNSET();
    antlr4::tree::TerminalNode* UPDATE();
    antlr4::tree::TerminalNode* USE();
    antlr4::tree::TerminalNode* USER();
    antlr4::tree::TerminalNode* VALUES();
    antlr4::tree::TerminalNode* VERSION();
    antlr4::tree::TerminalNode* VIEW();
    antlr4::tree::TerminalNode* VIEWS();
    antlr4::tree::TerminalNode* WEEK();
    antlr4::tree::TerminalNode* WEEKS();
    antlr4::tree::TerminalNode* WHEN();
    antlr4::tree::TerminalNode* WHERE();
    antlr4::tree::TerminalNode* WINDOW();
    antlr4::tree::TerminalNode* WITH();
    antlr4::tree::TerminalNode* WITHIN();
    antlr4::tree::TerminalNode* YEAR();
    antlr4::tree::TerminalNode* YEARS();
    antlr4::tree::TerminalNode* ZONE();

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NonReservedContext* nonReserved();

  bool sempred(
      antlr4::RuleContext* _localctx,
      size_t ruleIndex,
      size_t predicateIndex) override;

  bool queryTermSempred(QueryTermContext* _localctx, size_t predicateIndex);
  bool booleanExpressionSempred(
      BooleanExpressionContext* _localctx,
      size_t predicateIndex);
  bool valueExpressionSempred(
      ValueExpressionContext* _localctx,
      size_t predicateIndex);
  bool primaryExpressionSempred(
      PrimaryExpressionContext* _localctx,
      size_t predicateIndex);
  bool identifierSempred(IdentifierContext* _localctx, size_t predicateIndex);
  bool strictIdentifierSempred(
      StrictIdentifierContext* _localctx,
      size_t predicateIndex);
  bool quotedIdentifierSempred(
      QuotedIdentifierContext* _localctx,
      size_t predicateIndex);
  bool numberSempred(NumberContext* _localctx, size_t predicateIndex);
  bool stringLitSempred(StringLitContext* _localctx, size_t predicateIndex);

  // By default the static state used to implement the parser is lazily
  // initialized during the first call to the constructor. You can call this
  // function if you wish to initialize the static state ahead of time.
  static void initialize();

 private:
};

} // namespace axiom::sql::spark
