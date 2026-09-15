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

#include "PrestoSqlListener.h"
#include "PrestoSqlVisitor.h"

#include "PrestoSqlParser.h"

using namespace antlrcpp;

using namespace antlr4;

namespace {

struct PrestoSqlParserStaticData final {
  PrestoSqlParserStaticData(
      std::vector<std::string> ruleNames,
      std::vector<std::string> literalNames,
      std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)),
        literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  PrestoSqlParserStaticData(const PrestoSqlParserStaticData&) = delete;
  PrestoSqlParserStaticData(PrestoSqlParserStaticData&&) = delete;
  PrestoSqlParserStaticData& operator=(const PrestoSqlParserStaticData&) =
      delete;
  PrestoSqlParserStaticData& operator=(PrestoSqlParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag prestosqlParserOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
    std::unique_ptr<PrestoSqlParserStaticData>
        prestosqlParserStaticData = nullptr;

void prestosqlParserInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (prestosqlParserStaticData != nullptr) {
    return;
  }
#else
  assert(prestosqlParserStaticData == nullptr);
#endif
  auto staticData = std::make_unique<PrestoSqlParserStaticData>(
      std::vector<std::string>{
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
          "windowDefinition",
          "groupBy",
          "groupingElement",
          "groupingSet",
          "namedQuery",
          "setQuantifier",
          "selectItem",
          "starModifiers",
          "excludeClause",
          "replaceClause",
          "replaceItem",
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
          "windowSpecification",
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
          "nonReserved"},
      std::vector<std::string>{
          "",
          "'.'",
          "'('",
          "')'",
          "','",
          "'\\u003F'",
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
          "'EXCLUDE'",
          "'EXCLUDING'",
          "'EXECUTABLE'",
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
          "'GRAPH'",
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
          "'NULL'",
          "'NULLIF'",
          "'NULLS'",
          "'OF'",
          "'OFFSET'",
          "'ON'",
          "'ONLY'",
          "'OPTIMIZED'",
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
          "'WINDOW'",
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
          "'||'"},
      std::vector<std::string>{
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
          "EXCLUDE",
          "EXCLUDING",
          "EXECUTABLE",
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
          "GRAPH",
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
          "OPTIMIZED",
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
          "WINDOW",
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
          "DELIMITER"});
  static const int32_t serializedATNSegment[] = {
      4,    1,    264,  2237, 2,    0,    7,    0,    2,    1,    7,    1,
      2,    2,    7,    2,    2,    3,    7,    3,    2,    4,    7,    4,
      2,    5,    7,    5,    2,    6,    7,    6,    2,    7,    7,    7,
      2,    8,    7,    8,    2,    9,    7,    9,    2,    10,   7,    10,
      2,    11,   7,    11,   2,    12,   7,    12,   2,    13,   7,    13,
      2,    14,   7,    14,   2,    15,   7,    15,   2,    16,   7,    16,
      2,    17,   7,    17,   2,    18,   7,    18,   2,    19,   7,    19,
      2,    20,   7,    20,   2,    21,   7,    21,   2,    22,   7,    22,
      2,    23,   7,    23,   2,    24,   7,    24,   2,    25,   7,    25,
      2,    26,   7,    26,   2,    27,   7,    27,   2,    28,   7,    28,
      2,    29,   7,    29,   2,    30,   7,    30,   2,    31,   7,    31,
      2,    32,   7,    32,   2,    33,   7,    33,   2,    34,   7,    34,
      2,    35,   7,    35,   2,    36,   7,    36,   2,    37,   7,    37,
      2,    38,   7,    38,   2,    39,   7,    39,   2,    40,   7,    40,
      2,    41,   7,    41,   2,    42,   7,    42,   2,    43,   7,    43,
      2,    44,   7,    44,   2,    45,   7,    45,   2,    46,   7,    46,
      2,    47,   7,    47,   2,    48,   7,    48,   2,    49,   7,    49,
      2,    50,   7,    50,   2,    51,   7,    51,   2,    52,   7,    52,
      2,    53,   7,    53,   2,    54,   7,    54,   2,    55,   7,    55,
      2,    56,   7,    56,   2,    57,   7,    57,   2,    58,   7,    58,
      2,    59,   7,    59,   2,    60,   7,    60,   2,    61,   7,    61,
      2,    62,   7,    62,   2,    63,   7,    63,   2,    64,   7,    64,
      2,    65,   7,    65,   2,    66,   7,    66,   2,    67,   7,    67,
      2,    68,   7,    68,   2,    69,   7,    69,   2,    70,   7,    70,
      2,    71,   7,    71,   2,    72,   7,    72,   2,    73,   7,    73,
      2,    74,   7,    74,   2,    75,   7,    75,   2,    76,   7,    76,
      2,    77,   7,    77,   2,    78,   7,    78,   2,    79,   7,    79,
      2,    80,   7,    80,   2,    81,   7,    81,   2,    82,   7,    82,
      2,    83,   7,    83,   2,    84,   7,    84,   2,    85,   7,    85,
      2,    86,   7,    86,   2,    87,   7,    87,   2,    88,   7,    88,
      2,    89,   7,    89,   2,    90,   7,    90,   2,    91,   7,    91,
      2,    92,   7,    92,   2,    93,   7,    93,   2,    94,   7,    94,
      1,    0,    1,    0,    1,    0,    1,    1,    1,    1,    1,    1,
      1,    2,    1,    2,    1,    2,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    213,  8,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    218,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    224,  8,
      3,    1,    3,    1,    3,    3,    3,    228,  8,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,    3,
      242,  8,    3,    1,    3,    1,    3,    3,    3,    246,  8,    3,
      1,    3,    1,    3,    3,    3,    250,  8,    3,    1,    3,    1,
      3,    3,    3,    254,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    262,  8,    3,    1,
      3,    1,    3,    3,    3,    266,  8,    3,    1,    3,    3,    3,
      269,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    3,    3,    276,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    5,    3,    283,  8,    3,    10,   3,    12,
      3,    286,  9,    3,    1,    3,    1,    3,    1,    3,    3,    3,
      291,  8,    3,    1,    3,    1,    3,    3,    3,    295,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    301,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,
      3,    308,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    317,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    3,    3,    326,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      3,    3,    337,  8,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    344,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      3,    3,    354,  8,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    361,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    369,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    3,    3,    377,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    385,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    395,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    402,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,
      3,    410,  8,    3,    1,    3,    1,    3,    1,    3,    3,    3,
      415,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    426,
      8,    3,    1,    3,    1,    3,    1,    3,    3,    3,    431,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    442,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    453,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    5,    3,    463,  8,    3,    10,   3,    12,   3,
      466,  9,    3,    1,    3,    1,    3,    1,    3,    3,    3,    471,
      8,    3,    1,    3,    1,    3,    1,    3,    3,    3,    476,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    482,
      8,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    3,    3,    491,  8,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    502,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,    3,
      511,  8,    3,    1,    3,    1,    3,    1,    3,    3,    3,    516,
      8,    3,    1,    3,    1,    3,    3,    3,    520,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,
      3,    528,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    3,    3,    535,  8,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    3,    3,    548,  8,    3,    1,    3,
      3,    3,    551,  8,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    5,    3,    559,  8,    3,    10,   3,
      12,   3,    562,  9,    3,    3,    3,    564,  8,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    571,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    580,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    3,    3,    586,  8,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    591,  8,    3,    1,    3,    1,    3,
      3,    3,    595,  8,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    5,    3,    603,  8,    3,    10,   3,
      12,   3,    606,  9,    3,    3,    3,    608,  8,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    3,    3,    618,  8,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    5,    3,    629,  8,    3,    10,   3,    12,   3,    632,  9,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    637,  8,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    642,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    648,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    5,    3,
      655,  8,    3,    10,   3,    12,   3,    658,  9,    3,    1,    3,
      1,    3,    1,    3,    3,    3,    663,  8,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    670,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    5,    3,    676,  8,
      3,    10,   3,    12,   3,    679,  9,    3,    1,    3,    1,    3,
      3,    3,    683,  8,    3,    1,    3,    1,    3,    3,    3,    687,
      8,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    3,    3,    695,  8,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    701,  8,    3,    1,    3,    1,    3,
      1,    3,    5,    3,    706,  8,    3,    10,   3,    12,   3,    709,
      9,    3,    1,    3,    1,    3,    3,    3,    713,  8,    3,    1,
      3,    1,    3,    3,    3,    717,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      3,    3,    727,  8,    3,    1,    3,    3,    3,    730,  8,    3,
      1,    3,    1,    3,    3,    3,    734,  8,    3,    1,    3,    3,
      3,    737,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      5,    3,    743,  8,    3,    10,   3,    12,   3,    746,  9,    3,
      1,    3,    1,    3,    3,    3,    750,  8,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,
      3,    771,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      3,    3,    777,  8,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    3,    3,    783,  8,    3,    3,    3,    785,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    791,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    797,  8,
      3,    3,    3,    799,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    807,  8,    3,    3,
      3,    809,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    3,    3,    828,  8,    3,    1,    3,    1,    3,    1,
      3,    3,    3,    833,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    3,    3,    840,  8,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    3,    3,    852,  8,    3,    3,    3,
      854,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    862,  8,    3,    3,    3,    864,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    5,    3,    880,  8,    3,    10,   3,
      12,   3,    883,  9,    3,    3,    3,    885,  8,    3,    1,    3,
      1,    3,    3,    3,    889,  8,    3,    1,    3,    1,    3,    3,
      3,    893,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    5,    3,    909,  8,
      3,    10,   3,    12,   3,    912,  9,    3,    3,    3,    914,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    5,    3,    928,  8,    3,    10,   3,    12,   3,    931,  9,
      3,    1,    3,    1,    3,    3,    3,    935,  8,    3,    3,    3,
      937,  8,    3,    1,    4,    3,    4,    940,  8,    4,    1,    4,
      1,    4,    1,    5,    1,    5,    3,    5,    946,  8,    5,    1,
      5,    1,    5,    1,    5,    5,    5,    951,  8,    5,    10,   5,
      12,   5,    954,  9,    5,    1,    5,    3,    5,    957,  8,    5,
      1,    6,    1,    6,    1,    6,    3,    6,    962,  8,    6,    1,
      7,    1,    7,    1,    7,    1,    7,    3,    7,    968,  8,    7,
      1,    7,    1,    7,    3,    7,    972,  8,    7,    1,    7,    1,
      7,    3,    7,    976,  8,    7,    1,    8,    1,    8,    1,    8,
      1,    8,    3,    8,    982,  8,    8,    1,    9,    1,    9,    1,
      9,    1,    9,    5,    9,    988,  8,    9,    10,   9,    12,   9,
      991,  9,    9,    1,    9,    1,    9,    1,    10,   1,    10,   1,
      10,   1,    10,   1,    11,   1,    11,   1,    11,   1,    12,   5,
      12,   1003, 8,    12,   10,   12,   12,   12,   1006, 9,    12,   1,
      13,   1,    13,   1,    13,   1,    13,   3,    13,   1012, 8,    13,
      1,    14,   5,    14,   1015, 8,    14,   10,   14,   12,   14,   1018,
      9,    14,   1,    15,   1,    15,   1,    16,   1,    16,   3,    16,
      1024, 8,    16,   1,    17,   1,    17,   1,    17,   1,    18,   1,
      18,   1,    18,   3,    18,   1032, 8,    18,   1,    19,   1,    19,
      3,    19,   1036, 8,    19,   1,    20,   1,    20,   1,    20,   3,
      20,   1041, 8,    20,   1,    21,   1,    21,   1,    21,   1,    21,
      1,    21,   1,    21,   1,    21,   1,    21,   1,    21,   3,    21,
      1052, 8,    21,   1,    22,   1,    22,   1,    23,   1,    23,   1,
      23,   1,    23,   1,    23,   1,    23,   5,    23,   1062, 8,    23,
      10,   23,   12,   23,   1065, 9,    23,   3,    23,   1067, 8,    23,
      1,    23,   1,    23,   1,    23,   3,    23,   1072, 8,    23,   3,
      23,   1074, 8,    23,   1,    23,   1,    23,   1,    23,   1,    23,
      1,    23,   1,    23,   1,    23,   3,    23,   1083, 8,    23,   3,
      23,   1085, 8,    23,   1,    24,   1,    24,   1,    24,   1,    24,
      1,    24,   1,    24,   3,    24,   1093, 8,    24,   1,    24,   1,
      24,   1,    24,   1,    24,   3,    24,   1099, 8,    24,   1,    24,
      5,    24,   1102, 8,    24,   10,   24,   12,   24,   1105, 9,    24,
      1,    25,   1,    25,   1,    25,   1,    25,   1,    25,   1,    25,
      1,    25,   5,    25,   1114, 8,    25,   10,   25,   12,   25,   1117,
      9,    25,   1,    25,   3,    25,   1120, 8,    25,   1,    25,   1,
      25,   1,    25,   1,    25,   3,    25,   1126, 8,    25,   1,    26,
      1,    26,   3,    26,   1130, 8,    26,   1,    26,   1,    26,   3,
      26,   1134, 8,    26,   1,    27,   1,    27,   3,    27,   1138, 8,
      27,   1,    27,   1,    27,   1,    27,   5,    27,   1143, 8,    27,
      10,   27,   12,   27,   1146, 9,    27,   1,    27,   3,    27,   1149,
      8,    27,   1,    27,   1,    27,   1,    27,   1,    27,   5,    27,
      1155, 8,    27,   10,   27,   12,   27,   1158, 9,    27,   3,    27,
      1160, 8,    27,   1,    27,   1,    27,   3,    27,   1164, 8,    27,
      1,    27,   1,    27,   1,    27,   3,    27,   1169, 8,    27,   1,
      27,   1,    27,   3,    27,   1173, 8,    27,   1,    27,   1,    27,
      1,    27,   1,    27,   5,    27,   1179, 8,    27,   10,   27,   12,
      27,   1182, 9,    27,   3,    27,   1184, 8,    27,   1,    27,   1,
      27,   1,    27,   1,    27,   5,    27,   1190, 8,    27,   10,   27,
      12,   27,   1193, 9,    27,   1,    27,   1,    27,   3,    27,   1197,
      8,    27,   1,    27,   1,    27,   1,    27,   3,    27,   1202, 8,
      27,   1,    27,   1,    27,   3,    27,   1206, 8,    27,   1,    27,
      1,    27,   1,    27,   1,    27,   5,    27,   1212, 8,    27,   10,
      27,   12,   27,   1215, 9,    27,   3,    27,   1217, 8,    27,   3,
      27,   1219, 8,    27,   1,    28,   1,    28,   1,    28,   1,    28,
      1,    28,   1,    28,   1,    29,   3,    29,   1228, 8,    29,   1,
      29,   1,    29,   1,    29,   5,    29,   1233, 8,    29,   10,   29,
      12,   29,   1236, 9,    29,   1,    30,   1,    30,   1,    30,   1,
      30,   1,    30,   1,    30,   5,    30,   1244, 8,    30,   10,   30,
      12,   30,   1247, 9,    30,   3,    30,   1249, 8,    30,   1,    30,
      1,    30,   1,    30,   1,    30,   1,    30,   1,    30,   5,    30,
      1257, 8,    30,   10,   30,   12,   30,   1260, 9,    30,   3,    30,
      1262, 8,    30,   1,    30,   1,    30,   1,    30,   1,    30,   1,
      30,   1,    30,   1,    30,   5,    30,   1271, 8,    30,   10,   30,
      12,   30,   1274, 9,    30,   1,    30,   1,    30,   3,    30,   1278,
      8,    30,   1,    31,   1,    31,   1,    31,   1,    31,   5,    31,
      1284, 8,    31,   10,   31,   12,   31,   1287, 9,    31,   3,    31,
      1289, 8,    31,   1,    31,   1,    31,   3,    31,   1293, 8,    31,
      1,    32,   1,    32,   3,    32,   1297, 8,    32,   1,    32,   1,
      32,   1,    32,   1,    32,   1,    32,   1,    33,   1,    33,   1,
      34,   1,    34,   1,    34,   1,    34,   3,    34,   1310, 8,    34,
      1,    34,   1,    34,   3,    34,   1314, 8,    34,   1,    34,   1,
      34,   1,    34,   1,    34,   1,    34,   1,    34,   1,    34,   3,
      34,   1323, 8,    34,   1,    34,   1,    34,   1,    34,   1,    34,
      1,    34,   3,    34,   1330, 8,    34,   1,    34,   1,    34,   3,
      34,   1334, 8,    34,   1,    34,   3,    34,   1337, 8,    34,   3,
      34,   1339, 8,    34,   1,    35,   1,    35,   4,    35,   1343, 8,
      35,   11,   35,   12,   35,   1344, 1,    36,   1,    36,   1,    36,
      1,    36,   1,    36,   5,    36,   1352, 8,    36,   10,   36,   12,
      36,   1355, 9,    36,   1,    36,   1,    36,   1,    37,   1,    37,
      1,    37,   1,    37,   1,    37,   5,    37,   1364, 8,    37,   10,
      37,   12,   37,   1367, 9,    37,   1,    37,   1,    37,   1,    38,
      1,    38,   1,    38,   1,    38,   1,    39,   1,    39,   1,    39,
      1,    39,   1,    39,   1,    39,   1,    39,   1,    39,   1,    39,
      1,    39,   1,    39,   1,    39,   1,    39,   1,    39,   1,    39,
      1,    39,   1,    39,   3,    39,   1392, 8,    39,   5,    39,   1394,
      8,    39,   10,   39,   12,   39,   1397, 9,    39,   1,    40,   3,
      40,   1400, 8,    40,   1,    40,   1,    40,   3,    40,   1404, 8,
      40,   1,    40,   1,    40,   3,    40,   1408, 8,    40,   1,    40,
      1,    40,   3,    40,   1412, 8,    40,   3,    40,   1414, 8,    40,
      1,    41,   1,    41,   1,    41,   1,    41,   1,    41,   1,    41,
      1,    41,   5,    41,   1423, 8,    41,   10,   41,   12,   41,   1426,
      9,    41,   1,    41,   1,    41,   3,    41,   1430, 8,    41,   1,
      42,   1,    42,   1,    42,   1,    42,   1,    42,   1,    42,   1,
      42,   3,    42,   1439, 8,    42,   1,    43,   1,    43,   1,    44,
      1,    44,   3,    44,   1445, 8,    44,   1,    44,   1,    44,   3,
      44,   1449, 8,    44,   3,    44,   1451, 8,    44,   1,    45,   1,
      45,   1,    45,   1,    45,   5,    45,   1457, 8,    45,   10,   45,
      12,   45,   1460, 9,    45,   1,    45,   1,    45,   1,    46,   1,
      46,   3,    46,   1466, 8,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      5,    46,   1477, 8,    46,   10,   46,   12,   46,   1480, 9,    46,
      1,    46,   1,    46,   1,    46,   3,    46,   1485, 8,    46,   1,
      46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,
      46,   1,    46,   1,    46,   3,    46,   1496, 8,    46,   1,    47,
      1,    47,   1,    48,   1,    48,   1,    48,   3,    48,   1503, 8,
      48,   1,    48,   1,    48,   3,    48,   1507, 8,    48,   1,    48,
      1,    48,   1,    48,   1,    48,   1,    48,   1,    48,   5,    48,
      1515, 8,    48,   10,   48,   12,   48,   1518, 9,    48,   1,    49,
      1,    49,   1,    49,   1,    49,   1,    49,   1,    49,   1,    49,
      1,    49,   1,    49,   1,    49,   3,    49,   1530, 8,    49,   1,
      49,   1,    49,   1,    49,   1,    49,   1,    49,   1,    49,   3,
      49,   1538, 8,    49,   1,    49,   1,    49,   1,    49,   1,    49,
      1,    49,   5,    49,   1545, 8,    49,   10,   49,   12,   49,   1548,
      9,    49,   1,    49,   1,    49,   1,    49,   3,    49,   1553, 8,
      49,   1,    49,   1,    49,   1,    49,   1,    49,   1,    49,   1,
      49,   3,    49,   1561, 8,    49,   1,    49,   1,    49,   1,    49,
      1,    49,   3,    49,   1567, 8,    49,   1,    49,   1,    49,   3,
      49,   1571, 8,    49,   1,    49,   1,    49,   1,    49,   3,    49,
      1576, 8,    49,   1,    49,   1,    49,   1,    49,   3,    49,   1581,
      8,    49,   1,    50,   1,    50,   1,    50,   1,    50,   3,    50,
      1587, 8,    50,   1,    50,   1,    50,   1,    50,   1,    50,   1,
      50,   1,    50,   1,    50,   1,    50,   1,    50,   1,    50,   1,
      50,   1,    50,   5,    50,   1601, 8,    50,   10,   50,   12,   50,
      1604, 9,    50,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   4,    51,   1630, 8,    51,   11,   51,   12,   51,
      1631, 1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   5,    51,   1641, 8,    51,   10,   51,   12,   51,
      1644, 9,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   5,    51,   1658, 8,    51,   10,   51,   12,   51,
      1661, 9,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   3,    51,   1670, 8,    51,   1,    51,
      3,    51,   1673, 8,    51,   1,    51,   1,    51,   1,    51,   3,
      51,   1678, 8,    51,   1,    51,   1,    51,   1,    51,   5,    51,
      1683, 8,    51,   10,   51,   12,   51,   1686, 9,    51,   3,    51,
      1688, 8,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   5,    51,   1695, 8,    51,   10,   51,   12,   51,   1698, 9,
      51,   3,    51,   1700, 8,    51,   1,    51,   1,    51,   3,    51,
      1704, 8,    51,   1,    51,   3,    51,   1707, 8,    51,   1,    51,
      3,    51,   1710, 8,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   1,    51,   5,    51,   1720,
      8,    51,   10,   51,   12,   51,   1723, 9,    51,   3,    51,   1725,
      8,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,
      1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,
      1,    51,   1,    51,   1,    51,   1,    51,   4,    51,   1742, 8,
      51,   11,   51,   12,   51,   1743, 1,    51,   1,    51,   3,    51,
      1748, 8,    51,   1,    51,   1,    51,   1,    51,   1,    51,   4,
      51,   1754, 8,    51,   11,   51,   12,   51,   1755, 1,    51,   1,
      51,   3,    51,   1760, 8,    51,   1,    51,   1,    51,   1,    51,
      1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,
      1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,
      1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,
      5,    51,   1783, 8,    51,   10,   51,   12,   51,   1786, 9,    51,
      3,    51,   1788, 8,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   3,    51,   1797, 8,    51,
      1,    51,   1,    51,   1,    51,   1,    51,   3,    51,   1803, 8,
      51,   1,    51,   1,    51,   1,    51,   1,    51,   3,    51,   1809,
      8,    51,   1,    51,   1,    51,   1,    51,   1,    51,   3,    51,
      1815, 8,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   3,    51,   1825, 8,    51,
      1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,
      1,    51,   3,    51,   1834, 8,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   5,    51,   1854, 8,    51,
      10,   51,   12,   51,   1857, 9,    51,   3,    51,   1859, 8,    51,
      1,    51,   3,    51,   1862, 8,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   1,
      51,   1,    51,   1,    51,   1,    51,   5,    51,   1876, 8,    51,
      10,   51,   12,   51,   1879, 9,    51,   3,    51,   1881, 8,    51,
      1,    51,   1,    51,   1,    51,   1,    51,   1,    51,   5,    51,
      1888, 8,    51,   10,   51,   12,   51,   1891, 9,    51,   1,    52,
      1,    52,   1,    52,   1,    52,   3,    52,   1897, 8,    52,   3,
      52,   1899, 8,    52,   1,    53,   1,    53,   1,    53,   1,    53,
      3,    53,   1905, 8,    53,   1,    54,   1,    54,   1,    54,   1,
      54,   1,    54,   1,    54,   3,    54,   1913, 8,    54,   1,    55,
      1,    55,   1,    56,   1,    56,   1,    57,   1,    57,   1,    58,
      1,    58,   3,    58,   1923, 8,    58,   1,    58,   1,    58,   1,
      58,   1,    58,   3,    58,   1929, 8,    58,   1,    59,   1,    59,
      1,    60,   1,    60,   1,    61,   1,    61,   1,    61,   1,    61,
      5,    61,   1939, 8,    61,   10,   61,   12,   61,   1942, 9,    61,
      3,    61,   1944, 8,    61,   1,    61,   1,    61,   1,    62,   1,
      62,   1,    62,   1,    62,   1,    62,   1,    62,   1,    62,   1,
      62,   1,    62,   1,    62,   1,    62,   1,    62,   1,    62,   1,
      62,   1,    62,   1,    62,   1,    62,   1,    62,   1,    62,   1,
      62,   1,    62,   5,    62,   1969, 8,    62,   10,   62,   12,   62,
      1972, 9,    62,   1,    62,   1,    62,   1,    62,   1,    62,   1,
      62,   1,    62,   1,    62,   5,    62,   1981, 8,    62,   10,   62,
      12,   62,   1984, 9,    62,   1,    62,   1,    62,   3,    62,   1988,
      8,    62,   1,    62,   1,    62,   1,    62,   1,    62,   1,    62,
      3,    62,   1995, 8,    62,   1,    62,   1,    62,   5,    62,   1999,
      8,    62,   10,   62,   12,   62,   2002, 9,    62,   1,    63,   1,
      63,   3,    63,   2006, 8,    63,   1,    64,   1,    64,   1,    64,
      1,    64,   3,    64,   2012, 8,    64,   1,    65,   1,    65,   1,
      65,   1,    65,   1,    65,   1,    66,   1,    66,   1,    66,   1,
      66,   1,    66,   1,    66,   1,    67,   1,    67,   1,    67,   1,
      67,   1,    67,   1,    67,   1,    67,   3,    67,   2032, 8,    67,
      1,    68,   3,    68,   2035, 8,    68,   1,    68,   1,    68,   1,
      68,   1,    68,   1,    68,   5,    68,   2042, 8,    68,   10,   68,
      12,   68,   2045, 9,    68,   3,    68,   2047, 8,    68,   1,    68,
      1,    68,   1,    68,   1,    68,   1,    68,   5,    68,   2054, 8,
      68,   10,   68,   12,   68,   2057, 9,    68,   3,    68,   2059, 8,
      68,   1,    68,   3,    68,   2062, 8,    68,   1,    69,   1,    69,
      1,    69,   1,    69,   1,    69,   1,    69,   1,    69,   1,    69,
      1,    69,   1,    69,   1,    69,   1,    69,   1,    69,   1,    69,
      1,    69,   1,    69,   1,    69,   1,    69,   1,    69,   1,    69,
      1,    69,   1,    69,   1,    69,   1,    69,   3,    69,   2088, 8,
      69,   1,    70,   1,    70,   1,    70,   1,    70,   1,    70,   1,
      70,   1,    70,   1,    70,   1,    70,   3,    70,   2099, 8,    70,
      1,    71,   1,    71,   1,    71,   1,    71,   1,    72,   1,    72,
      1,    72,   1,    72,   1,    72,   1,    72,   3,    72,   2111, 8,
      72,   3,    72,   2113, 8,    72,   1,    73,   1,    73,   1,    73,
      1,    73,   1,    73,   3,    73,   2120, 8,    73,   1,    74,   1,
      74,   1,    74,   1,    74,   1,    74,   1,    74,   1,    74,   3,
      74,   2129, 8,    74,   1,    75,   1,    75,   1,    75,   1,    75,
      1,    75,   3,    75,   2136, 8,    75,   1,    76,   1,    76,   1,
      76,   1,    76,   3,    76,   2142, 8,    76,   1,    77,   1,    77,
      1,    77,   5,    77,   2147, 8,    77,   10,   77,   12,   77,   2150,
      9,    77,   1,    78,   1,    78,   1,    78,   1,    78,   1,    78,
      1,    79,   1,    79,   1,    79,   3,    79,   2160, 8,    79,   1,
      80,   1,    80,   1,    80,   3,    80,   2165, 8,    80,   1,    81,
      1,    81,   1,    81,   1,    81,   1,    81,   3,    81,   2172, 8,
      81,   1,    82,   1,    82,   1,    82,   5,    82,   2177, 8,    82,
      10,   82,   12,   82,   2180, 9,    82,   1,    83,   1,    83,   1,
      83,   1,    83,   1,    83,   3,    83,   2187, 8,    83,   1,    84,
      1,    84,   1,    84,   3,    84,   2192, 8,    84,   1,    85,   1,
      85,   3,    85,   2196, 8,    85,   1,    86,   1,    86,   1,    86,
      1,    86,   1,    87,   1,    87,   1,    87,   3,    87,   2205, 8,
      87,   1,    88,   1,    88,   1,    88,   3,    88,   2210, 8,    88,
      1,    89,   5,    89,   2213, 8,    89,   10,   89,   12,   89,   2216,
      9,    89,   1,    90,   1,    90,   1,    90,   3,    90,   2221, 8,
      90,   1,    91,   1,    91,   1,    91,   3,    91,   2226, 8,    91,
      1,    92,   1,    92,   1,    93,   1,    93,   1,    93,   3,    93,
      2233, 8,    93,   1,    94,   1,    94,   1,    94,   0,    6,    48,
      78,   96,   100,  102,  124,  95,   0,    2,    4,    6,    8,    10,
      12,   14,   16,   18,   20,   22,   24,   26,   28,   30,   32,   34,
      36,   38,   40,   42,   44,   46,   48,   50,   52,   54,   56,   58,
      60,   62,   64,   66,   68,   70,   72,   74,   76,   78,   80,   82,
      84,   86,   88,   90,   92,   94,   96,   98,   100,  102,  104,  106,
      108,  110,  112,  114,  116,  118,  120,  122,  124,  126,  128,  130,
      132,  134,  136,  138,  140,  142,  144,  146,  148,  150,  152,  154,
      156,  158,  160,  162,  164,  166,  168,  170,  172,  174,  176,  178,
      180,  182,  184,  186,  188,  0,    25,   2,    0,    28,   28,   169,
      169,  2,    0,    51,   51,   105,  105,  2,    0,    81,   81,   97,
      97,   2,    0,    67,   67,   98,   98,   1,    0,    178,  179,  2,
      0,    13,   13,   250,  250,  2,    0,    65,   65,   215,  215,  2,
      0,    20,   20,   53,   53,   2,    0,    77,   77,   113,  113,  2,
      0,    13,   13,   57,   57,   2,    0,    23,   23,   195,  195,  1,
      0,    241,  242,  1,    0,    243,  245,  1,    0,    235,  240,  3,
      0,    13,   13,   17,   17,   190,  190,  2,    0,    74,   74,   208,
      208,  5,    0,    49,   49,   94,   94,   124,  125,  182,  182,  233,
      233,  1,    0,    128,  131,  2,    0,    78,   78,   154,  154,  3,
      0,    89,   89,   109,  109,  202,  202,  7,    0,    58,   58,   68,
      68,   88,   88,   106,  106,  121,  121,  143,  143,  222,  222,  2,
      0,    142,  142,  232,  232,  3,    0,    196,  197,  205,  205,  225,
      225,  2,    0,    56,   56,   61,   61,   51,   0,    11,   13,   15,
      15,   17,   18,   20,   23,   26,   28,   31,   36,   41,   41,   43,
      43,   47,   49,   51,   51,   53,   53,   55,   56,   58,   58,   61,
      61,   63,   63,   66,   68,   71,   71,   73,   73,   75,   78,   80,
      80,   83,   89,   92,   92,   94,   96,   98,   98,   100,  100,  103,
      103,  105,  106,  108,  109,  111,  114,  116,  116,  118,  118,  121,
      126,  128,  133,  137,  140,  142,  144,  147,  147,  149,  154,  156,
      160,  162,  172,  174,  176,  178,  183,  185,  197,  199,  202,  204,
      207,  209,  211,  213,  214,  216,  216,  218,  220,  222,  222,  224,
      226,  230,  234,  2577, 0,    190,  1,    0,    0,    0,    2,    193,
      1,    0,    0,    0,    4,    196,  1,    0,    0,    0,    6,    936,
      1,    0,    0,    0,    8,    939,  1,    0,    0,    0,    10,   943,
      1,    0,    0,    0,    12,   961,  1,    0,    0,    0,    14,   963,
      1,    0,    0,    0,    16,   977,  1,    0,    0,    0,    18,   983,
      1,    0,    0,    0,    20,   994,  1,    0,    0,    0,    22,   998,
      1,    0,    0,    0,    24,   1004, 1,    0,    0,    0,    26,   1011,
      1,    0,    0,    0,    28,   1016, 1,    0,    0,    0,    30,   1019,
      1,    0,    0,    0,    32,   1023, 1,    0,    0,    0,    34,   1025,
      1,    0,    0,    0,    36,   1028, 1,    0,    0,    0,    38,   1035,
      1,    0,    0,    0,    40,   1040, 1,    0,    0,    0,    42,   1051,
      1,    0,    0,    0,    44,   1053, 1,    0,    0,    0,    46,   1055,
      1,    0,    0,    0,    48,   1086, 1,    0,    0,    0,    50,   1125,
      1,    0,    0,    0,    52,   1127, 1,    0,    0,    0,    54,   1218,
      1,    0,    0,    0,    56,   1220, 1,    0,    0,    0,    58,   1227,
      1,    0,    0,    0,    60,   1277, 1,    0,    0,    0,    62,   1292,
      1,    0,    0,    0,    64,   1294, 1,    0,    0,    0,    66,   1303,
      1,    0,    0,    0,    68,   1338, 1,    0,    0,    0,    70,   1342,
      1,    0,    0,    0,    72,   1346, 1,    0,    0,    0,    74,   1358,
      1,    0,    0,    0,    76,   1370, 1,    0,    0,    0,    78,   1374,
      1,    0,    0,    0,    80,   1413, 1,    0,    0,    0,    82,   1429,
      1,    0,    0,    0,    84,   1431, 1,    0,    0,    0,    86,   1440,
      1,    0,    0,    0,    88,   1442, 1,    0,    0,    0,    90,   1452,
      1,    0,    0,    0,    92,   1495, 1,    0,    0,    0,    94,   1497,
      1,    0,    0,    0,    96,   1506, 1,    0,    0,    0,    98,   1580,
      1,    0,    0,    0,    100,  1586, 1,    0,    0,    0,    102,  1861,
      1,    0,    0,    0,    104,  1898, 1,    0,    0,    0,    106,  1904,
      1,    0,    0,    0,    108,  1912, 1,    0,    0,    0,    110,  1914,
      1,    0,    0,    0,    112,  1916, 1,    0,    0,    0,    114,  1918,
      1,    0,    0,    0,    116,  1920, 1,    0,    0,    0,    118,  1930,
      1,    0,    0,    0,    120,  1932, 1,    0,    0,    0,    122,  1934,
      1,    0,    0,    0,    124,  1994, 1,    0,    0,    0,    126,  2005,
      1,    0,    0,    0,    128,  2011, 1,    0,    0,    0,    130,  2013,
      1,    0,    0,    0,    132,  2018, 1,    0,    0,    0,    134,  2031,
      1,    0,    0,    0,    136,  2034, 1,    0,    0,    0,    138,  2087,
      1,    0,    0,    0,    140,  2098, 1,    0,    0,    0,    142,  2100,
      1,    0,    0,    0,    144,  2112, 1,    0,    0,    0,    146,  2119,
      1,    0,    0,    0,    148,  2128, 1,    0,    0,    0,    150,  2135,
      1,    0,    0,    0,    152,  2141, 1,    0,    0,    0,    154,  2143,
      1,    0,    0,    0,    156,  2151, 1,    0,    0,    0,    158,  2159,
      1,    0,    0,    0,    160,  2164, 1,    0,    0,    0,    162,  2171,
      1,    0,    0,    0,    164,  2173, 1,    0,    0,    0,    166,  2186,
      1,    0,    0,    0,    168,  2191, 1,    0,    0,    0,    170,  2195,
      1,    0,    0,    0,    172,  2197, 1,    0,    0,    0,    174,  2201,
      1,    0,    0,    0,    176,  2209, 1,    0,    0,    0,    178,  2214,
      1,    0,    0,    0,    180,  2220, 1,    0,    0,    0,    182,  2225,
      1,    0,    0,    0,    184,  2227, 1,    0,    0,    0,    186,  2232,
      1,    0,    0,    0,    188,  2234, 1,    0,    0,    0,    190,  191,
      3,    6,    3,    0,    191,  192,  5,    0,    0,    1,    192,  1,
      1,    0,    0,    0,    193,  194,  3,    94,   47,   0,    194,  195,
      5,    0,    0,    1,    195,  3,    1,    0,    0,    0,    196,  197,
      3,    32,   16,   0,    197,  198,  5,    0,    0,    1,    198,  5,
      1,    0,    0,    0,    199,  937,  3,    8,    4,    0,    200,  201,
      5,    219,  0,    0,    201,  937,  3,    166,  83,   0,    202,  203,
      5,    219,  0,    0,    203,  204,  3,    166,  83,   0,    204,  205,
      5,    1,    0,    0,    205,  206,  3,    166,  83,   0,    206,  937,
      1,    0,    0,    0,    207,  208,  5,    38,   0,    0,    208,  212,
      5,    180,  0,    0,    209,  210,  5,    95,   0,    0,    210,  211,
      5,    135,  0,    0,    211,  213,  5,    70,   0,    0,    212,  209,
      1,    0,    0,    0,    212,  213,  1,    0,    0,    0,    213,  214,
      1,    0,    0,    0,    214,  217,  3,    154,  77,   0,    215,  216,
      5,    229,  0,    0,    216,  218,  3,    18,   9,    0,    217,  215,
      1,    0,    0,    0,    217,  218,  1,    0,    0,    0,    218,  937,
      1,    0,    0,    0,    219,  220,  5,    59,   0,    0,    220,  223,
      5,    180,  0,    0,    221,  222,  5,    95,   0,    0,    222,  224,
      5,    70,   0,    0,    223,  221,  1,    0,    0,    0,    223,  224,
      1,    0,    0,    0,    224,  225,  1,    0,    0,    0,    225,  227,
      3,    154,  77,   0,    226,  228,  7,    0,    0,    0,    227,  226,
      1,    0,    0,    0,    227,  228,  1,    0,    0,    0,    228,  937,
      1,    0,    0,    0,    229,  230,  5,    14,   0,    0,    230,  231,
      5,    180,  0,    0,    231,  232,  3,    154,  77,   0,    232,  233,
      5,    164,  0,    0,    233,  234,  5,    206,  0,    0,    234,  235,
      3,    166,  83,   0,    235,  937,  1,    0,    0,    0,    236,  237,
      5,    38,   0,    0,    237,  241,  5,    198,  0,    0,    238,  239,
      5,    95,   0,    0,    239,  240,  5,    135,  0,    0,    240,  242,
      5,    70,   0,    0,    241,  238,  1,    0,    0,    0,    241,  242,
      1,    0,    0,    0,    242,  243,  1,    0,    0,    0,    243,  245,
      3,    154,  77,   0,    244,  246,  3,    90,   45,   0,    245,  244,
      1,    0,    0,    0,    245,  246,  1,    0,    0,    0,    246,  249,
      1,    0,    0,    0,    247,  248,  5,    34,   0,    0,    248,  250,
      3,    104,  52,   0,    249,  247,  1,    0,    0,    0,    249,  250,
      1,    0,    0,    0,    250,  253,  1,    0,    0,    0,    251,  252,
      5,    229,  0,    0,    252,  254,  3,    18,   9,    0,    253,  251,
      1,    0,    0,    0,    253,  254,  1,    0,    0,    0,    254,  255,
      1,    0,    0,    0,    255,  261,  5,    19,   0,    0,    256,  262,
      3,    8,    4,    0,    257,  258,  5,    2,    0,    0,    258,  259,
      3,    8,    4,    0,    259,  260,  5,    3,    0,    0,    260,  262,
      1,    0,    0,    0,    261,  256,  1,    0,    0,    0,    261,  257,
      1,    0,    0,    0,    262,  268,  1,    0,    0,    0,    263,  265,
      5,    229,  0,    0,    264,  266,  5,    132,  0,    0,    265,  264,
      1,    0,    0,    0,    265,  266,  1,    0,    0,    0,    266,  267,
      1,    0,    0,    0,    267,  269,  5,    47,   0,    0,    268,  263,
      1,    0,    0,    0,    268,  269,  1,    0,    0,    0,    269,  937,
      1,    0,    0,    0,    270,  271,  5,    38,   0,    0,    271,  275,
      5,    198,  0,    0,    272,  273,  5,    95,   0,    0,    273,  274,
      5,    135,  0,    0,    274,  276,  5,    70,   0,    0,    275,  272,
      1,    0,    0,    0,    275,  276,  1,    0,    0,    0,    276,  277,
      1,    0,    0,    0,    277,  278,  3,    154,  77,   0,    278,  279,
      5,    2,    0,    0,    279,  284,  3,    12,   6,    0,    280,  281,
      5,    4,    0,    0,    281,  283,  3,    12,   6,    0,    282,  280,
      1,    0,    0,    0,    283,  286,  1,    0,    0,    0,    284,  282,
      1,    0,    0,    0,    284,  285,  1,    0,    0,    0,    285,  287,
      1,    0,    0,    0,    286,  284,  1,    0,    0,    0,    287,  290,
      5,    3,    0,    0,    288,  289,  5,    34,   0,    0,    289,  291,
      3,    104,  52,   0,    290,  288,  1,    0,    0,    0,    290,  291,
      1,    0,    0,    0,    291,  294,  1,    0,    0,    0,    292,  293,
      5,    229,  0,    0,    293,  295,  3,    18,   9,    0,    294,  292,
      1,    0,    0,    0,    294,  295,  1,    0,    0,    0,    295,  937,
      1,    0,    0,    0,    296,  297,  5,    59,   0,    0,    297,  300,
      5,    198,  0,    0,    298,  299,  5,    95,   0,    0,    299,  301,
      5,    70,   0,    0,    300,  298,  1,    0,    0,    0,    300,  301,
      1,    0,    0,    0,    301,  302,  1,    0,    0,    0,    302,  937,
      3,    154,  77,   0,    303,  304,  5,    101,  0,    0,    304,  305,
      5,    104,  0,    0,    305,  307,  3,    154,  77,   0,    306,  308,
      3,    90,   45,   0,    307,  306,  1,    0,    0,    0,    307,  308,
      1,    0,    0,    0,    308,  309,  1,    0,    0,    0,    309,  310,
      3,    8,    4,    0,    310,  937,  1,    0,    0,    0,    311,  312,
      5,    52,   0,    0,    312,  313,  5,    81,   0,    0,    313,  316,
      3,    154,  77,   0,    314,  315,  5,    228,  0,    0,    315,  317,
      3,    96,   48,   0,    316,  314,  1,    0,    0,    0,    316,  317,
      1,    0,    0,    0,    317,  937,  1,    0,    0,    0,    318,  319,
      5,    209,  0,    0,    319,  320,  5,    198,  0,    0,    320,  937,
      3,    154,  77,   0,    321,  322,  5,    14,   0,    0,    322,  325,
      5,    198,  0,    0,    323,  324,  5,    95,   0,    0,    324,  326,
      5,    70,   0,    0,    325,  323,  1,    0,    0,    0,    325,  326,
      1,    0,    0,    0,    326,  327,  1,    0,    0,    0,    327,  328,
      3,    154,  77,   0,    328,  329,  5,    164,  0,    0,    329,  330,
      5,    206,  0,    0,    330,  331,  3,    154,  77,   0,    331,  937,
      1,    0,    0,    0,    332,  333,  5,    14,   0,    0,    333,  336,
      5,    198,  0,    0,    334,  335,  5,    95,   0,    0,    335,  337,
      5,    70,   0,    0,    336,  334,  1,    0,    0,    0,    336,  337,
      1,    0,    0,    0,    337,  338,  1,    0,    0,    0,    338,  339,
      3,    154,  77,   0,    339,  340,  5,    164,  0,    0,    340,  343,
      5,    32,   0,    0,    341,  342,  5,    95,   0,    0,    342,  344,
      5,    70,   0,    0,    343,  341,  1,    0,    0,    0,    343,  344,
      1,    0,    0,    0,    344,  345,  1,    0,    0,    0,    345,  346,
      3,    166,  83,   0,    346,  347,  5,    206,  0,    0,    347,  348,
      3,    166,  83,   0,    348,  937,  1,    0,    0,    0,    349,  350,
      5,    14,   0,    0,    350,  353,  5,    198,  0,    0,    351,  352,
      5,    95,   0,    0,    352,  354,  5,    70,   0,    0,    353,  351,
      1,    0,    0,    0,    353,  354,  1,    0,    0,    0,    354,  355,
      1,    0,    0,    0,    355,  356,  3,    154,  77,   0,    356,  357,
      5,    59,   0,    0,    357,  360,  5,    32,   0,    0,    358,  359,
      5,    95,   0,    0,    359,  361,  5,    70,   0,    0,    360,  358,
      1,    0,    0,    0,    360,  361,  1,    0,    0,    0,    361,  362,
      1,    0,    0,    0,    362,  363,  3,    154,  77,   0,    363,  937,
      1,    0,    0,    0,    364,  365,  5,    14,   0,    0,    365,  368,
      5,    198,  0,    0,    366,  367,  5,    95,   0,    0,    367,  369,
      5,    70,   0,    0,    368,  366,  1,    0,    0,    0,    368,  369,
      1,    0,    0,    0,    369,  370,  1,    0,    0,    0,    370,  371,
      3,    154,  77,   0,    371,  372,  5,    11,   0,    0,    372,  376,
      5,    32,   0,    0,    373,  374,  5,    95,   0,    0,    374,  375,
      5,    135,  0,    0,    375,  377,  5,    70,   0,    0,    376,  373,
      1,    0,    0,    0,    376,  377,  1,    0,    0,    0,    377,  378,
      1,    0,    0,    0,    378,  379,  3,    14,   7,    0,    379,  937,
      1,    0,    0,    0,    380,  381,  5,    14,   0,    0,    381,  384,
      5,    198,  0,    0,    382,  383,  5,    95,   0,    0,    383,  385,
      5,    70,   0,    0,    384,  382,  1,    0,    0,    0,    384,  385,
      1,    0,    0,    0,    385,  386,  1,    0,    0,    0,    386,  387,
      3,    154,  77,   0,    387,  388,  5,    11,   0,    0,    388,  389,
      3,    170,  85,   0,    389,  937,  1,    0,    0,    0,    390,  391,
      5,    14,   0,    0,    391,  394,  5,    198,  0,    0,    392,  393,
      5,    95,   0,    0,    393,  395,  5,    70,   0,    0,    394,  392,
      1,    0,    0,    0,    394,  395,  1,    0,    0,    0,    395,  396,
      1,    0,    0,    0,    396,  397,  3,    154,  77,   0,    397,  398,
      5,    59,   0,    0,    398,  401,  5,    37,   0,    0,    399,  400,
      5,    95,   0,    0,    400,  402,  5,    70,   0,    0,    401,  399,
      1,    0,    0,    0,    401,  402,  1,    0,    0,    0,    402,  403,
      1,    0,    0,    0,    403,  404,  3,    166,  83,   0,    404,  937,
      1,    0,    0,    0,    405,  406,  5,    14,   0,    0,    406,  409,
      5,    198,  0,    0,    407,  408,  5,    95,   0,    0,    408,  410,
      5,    70,   0,    0,    409,  407,  1,    0,    0,    0,    409,  410,
      1,    0,    0,    0,    410,  411,  1,    0,    0,    0,    411,  412,
      3,    154,  77,   0,    412,  414,  5,    14,   0,    0,    413,  415,
      5,    32,   0,    0,    414,  413,  1,    0,    0,    0,    414,  415,
      1,    0,    0,    0,    415,  416,  1,    0,    0,    0,    416,  417,
      3,    166,  83,   0,    417,  418,  5,    187,  0,    0,    418,  419,
      5,    135,  0,    0,    419,  420,  5,    136,  0,    0,    420,  937,
      1,    0,    0,    0,    421,  422,  5,    14,   0,    0,    422,  425,
      5,    198,  0,    0,    423,  424,  5,    95,   0,    0,    424,  426,
      5,    70,   0,    0,    425,  423,  1,    0,    0,    0,    425,  426,
      1,    0,    0,    0,    426,  427,  1,    0,    0,    0,    427,  428,
      3,    154,  77,   0,    428,  430,  5,    14,   0,    0,    429,  431,
      5,    32,   0,    0,    430,  429,  1,    0,    0,    0,    430,  431,
      1,    0,    0,    0,    431,  432,  1,    0,    0,    0,    432,  433,
      3,    166,  83,   0,    433,  434,  5,    59,   0,    0,    434,  435,
      5,    135,  0,    0,    435,  436,  5,    136,  0,    0,    436,  937,
      1,    0,    0,    0,    437,  438,  5,    14,   0,    0,    438,  441,
      5,    198,  0,    0,    439,  440,  5,    95,   0,    0,    440,  442,
      5,    70,   0,    0,    441,  439,  1,    0,    0,    0,    441,  442,
      1,    0,    0,    0,    442,  443,  1,    0,    0,    0,    443,  444,
      3,    154,  77,   0,    444,  445,  5,    187,  0,    0,    445,  446,
      5,    158,  0,    0,    446,  447,  3,    18,   9,    0,    447,  937,
      1,    0,    0,    0,    448,  449,  5,    15,   0,    0,    449,  452,
      3,    154,  77,   0,    450,  451,  5,    229,  0,    0,    451,  453,
      3,    18,   9,    0,    452,  450,  1,    0,    0,    0,    452,  453,
      1,    0,    0,    0,    453,  937,  1,    0,    0,    0,    454,  455,
      5,    38,   0,    0,    455,  456,  5,    211,  0,    0,    456,  457,
      3,    154,  77,   0,    457,  470,  5,    19,   0,    0,    458,  459,
      5,    2,    0,    0,    459,  464,  3,    22,   11,   0,    460,  461,
      5,    4,    0,    0,    461,  463,  3,    22,   11,   0,    462,  460,
      1,    0,    0,    0,    463,  466,  1,    0,    0,    0,    464,  462,
      1,    0,    0,    0,    464,  465,  1,    0,    0,    0,    465,  467,
      1,    0,    0,    0,    466,  464,  1,    0,    0,    0,    467,  468,
      5,    3,    0,    0,    468,  471,  1,    0,    0,    0,    469,  471,
      3,    124,  62,   0,    470,  458,  1,    0,    0,    0,    470,  469,
      1,    0,    0,    0,    471,  937,  1,    0,    0,    0,    472,  475,
      5,    38,   0,    0,    473,  474,  5,    145,  0,    0,    474,  476,
      5,    166,  0,    0,    475,  473,  1,    0,    0,    0,    475,  476,
      1,    0,    0,    0,    476,  477,  1,    0,    0,    0,    477,  478,
      5,    226,  0,    0,    478,  481,  3,    154,  77,   0,    479,  480,
      5,    183,  0,    0,    480,  482,  7,    1,    0,    0,    481,  479,
      1,    0,    0,    0,    481,  482,  1,    0,    0,    0,    482,  483,
      1,    0,    0,    0,    483,  484,  5,    19,   0,    0,    484,  485,
      3,    8,    4,    0,    485,  937,  1,    0,    0,    0,    486,  487,
      5,    14,   0,    0,    487,  490,  5,    226,  0,    0,    488,  489,
      5,    95,   0,    0,    489,  491,  5,    70,   0,    0,    490,  488,
      1,    0,    0,    0,    490,  491,  1,    0,    0,    0,    491,  492,
      1,    0,    0,    0,    492,  493,  3,    154,  77,   0,    493,  494,
      5,    164,  0,    0,    494,  495,  5,    206,  0,    0,    495,  496,
      3,    154,  77,   0,    496,  937,  1,    0,    0,    0,    497,  498,
      5,    59,   0,    0,    498,  501,  5,    226,  0,    0,    499,  500,
      5,    95,   0,    0,    500,  502,  5,    70,   0,    0,    501,  499,
      1,    0,    0,    0,    501,  502,  1,    0,    0,    0,    502,  503,
      1,    0,    0,    0,    503,  937,  3,    154,  77,   0,    504,  505,
      5,    38,   0,    0,    505,  506,  5,    123,  0,    0,    506,  510,
      5,    226,  0,    0,    507,  508,  5,    95,   0,    0,    508,  509,
      5,    135,  0,    0,    509,  511,  5,    70,   0,    0,    510,  507,
      1,    0,    0,    0,    510,  511,  1,    0,    0,    0,    511,  512,
      1,    0,    0,    0,    512,  515,  3,    154,  77,   0,    513,  514,
      5,    34,   0,    0,    514,  516,  3,    104,  52,   0,    515,  513,
      1,    0,    0,    0,    515,  516,  1,    0,    0,    0,    516,  519,
      1,    0,    0,    0,    517,  518,  5,    229,  0,    0,    518,  520,
      3,    18,   9,    0,    519,  517,  1,    0,    0,    0,    519,  520,
      1,    0,    0,    0,    520,  521,  1,    0,    0,    0,    521,  527,
      5,    19,   0,    0,    522,  528,  3,    8,    4,    0,    523,  524,
      5,    2,    0,    0,    524,  525,  3,    8,    4,    0,    525,  526,
      5,    3,    0,    0,    526,  528,  1,    0,    0,    0,    527,  522,
      1,    0,    0,    0,    527,  523,  1,    0,    0,    0,    528,  937,
      1,    0,    0,    0,    529,  530,  5,    59,   0,    0,    530,  531,
      5,    123,  0,    0,    531,  534,  5,    226,  0,    0,    532,  533,
      5,    95,   0,    0,    533,  535,  5,    70,   0,    0,    534,  532,
      1,    0,    0,    0,    534,  535,  1,    0,    0,    0,    535,  536,
      1,    0,    0,    0,    536,  937,  3,    154,  77,   0,    537,  538,
      5,    162,  0,    0,    538,  539,  5,    123,  0,    0,    539,  540,
      5,    226,  0,    0,    540,  541,  3,    154,  77,   0,    541,  542,
      5,    228,  0,    0,    542,  543,  3,    96,   48,   0,    543,  937,
      1,    0,    0,    0,    544,  547,  5,    38,   0,    0,    545,  546,
      5,    145,  0,    0,    546,  548,  5,    166,  0,    0,    547,  545,
      1,    0,    0,    0,    547,  548,  1,    0,    0,    0,    548,  550,
      1,    0,    0,    0,    549,  551,  5,    201,  0,    0,    550,  549,
      1,    0,    0,    0,    550,  551,  1,    0,    0,    0,    551,  552,
      1,    0,    0,    0,    552,  553,  5,    83,   0,    0,    553,  554,
      3,    154,  77,   0,    554,  563,  5,    2,    0,    0,    555,  560,
      3,    22,   11,   0,    556,  557,  5,    4,    0,    0,    557,  559,
      3,    22,   11,   0,    558,  556,  1,    0,    0,    0,    559,  562,
      1,    0,    0,    0,    560,  558,  1,    0,    0,    0,    560,  561,
      1,    0,    0,    0,    561,  564,  1,    0,    0,    0,    562,  560,
      1,    0,    0,    0,    563,  555,  1,    0,    0,    0,    563,  564,
      1,    0,    0,    0,    564,  565,  1,    0,    0,    0,    565,  566,
      5,    3,    0,    0,    566,  567,  5,    171,  0,    0,    567,  570,
      3,    124,  62,   0,    568,  569,  5,    34,   0,    0,    569,  571,
      3,    104,  52,   0,    570,  568,  1,    0,    0,    0,    570,  571,
      1,    0,    0,    0,    571,  572,  1,    0,    0,    0,    572,  573,
      3,    24,   12,   0,    573,  574,  3,    32,   16,   0,    574,  937,
      1,    0,    0,    0,    575,  576,  5,    14,   0,    0,    576,  577,
      5,    83,   0,    0,    577,  579,  3,    154,  77,   0,    578,  580,
      3,    122,  61,   0,    579,  578,  1,    0,    0,    0,    579,  580,
      1,    0,    0,    0,    580,  581,  1,    0,    0,    0,    581,  582,
      3,    28,   14,   0,    582,  937,  1,    0,    0,    0,    583,  585,
      5,    59,   0,    0,    584,  586,  5,    201,  0,    0,    585,  584,
      1,    0,    0,    0,    585,  586,  1,    0,    0,    0,    586,  587,
      1,    0,    0,    0,    587,  590,  5,    83,   0,    0,    588,  589,
      5,    95,   0,    0,    589,  591,  5,    70,   0,    0,    590,  588,
      1,    0,    0,    0,    590,  591,  1,    0,    0,    0,    591,  592,
      1,    0,    0,    0,    592,  594,  3,    154,  77,   0,    593,  595,
      3,    122,  61,   0,    594,  593,  1,    0,    0,    0,    594,  595,
      1,    0,    0,    0,    595,  937,  1,    0,    0,    0,    596,  597,
      5,    26,   0,    0,    597,  598,  3,    154,  77,   0,    598,  607,
      5,    2,    0,    0,    599,  604,  3,    150,  75,   0,    600,  601,
      5,    4,    0,    0,    601,  603,  3,    150,  75,   0,    602,  600,
      1,    0,    0,    0,    603,  606,  1,    0,    0,    0,    604,  602,
      1,    0,    0,    0,    604,  605,  1,    0,    0,    0,    605,  608,
      1,    0,    0,    0,    606,  604,  1,    0,    0,    0,    607,  599,
      1,    0,    0,    0,    607,  608,  1,    0,    0,    0,    608,  609,
      1,    0,    0,    0,    609,  610,  5,    3,    0,    0,    610,  937,
      1,    0,    0,    0,    611,  612,  5,    38,   0,    0,    612,  613,
      5,    174,  0,    0,    613,  617,  3,    166,  83,   0,    614,  615,
      5,    229,  0,    0,    615,  616,  5,    12,   0,    0,    616,  618,
      3,    160,  80,   0,    617,  614,  1,    0,    0,    0,    617,  618,
      1,    0,    0,    0,    618,  937,  1,    0,    0,    0,    619,  620,
      5,    59,   0,    0,    620,  621,  5,    174,  0,    0,    621,  937,
      3,    166,  83,   0,    622,  623,  5,    85,   0,    0,    623,  624,
      3,    164,  82,   0,    624,  625,  5,    206,  0,    0,    625,  630,
      3,    162,  81,   0,    626,  627,  5,    4,    0,    0,    627,  629,
      3,    162,  81,   0,    628,  626,  1,    0,    0,    0,    629,  632,
      1,    0,    0,    0,    630,  628,  1,    0,    0,    0,    630,  631,
      1,    0,    0,    0,    631,  636,  1,    0,    0,    0,    632,  630,
      1,    0,    0,    0,    633,  634,  5,    229,  0,    0,    634,  635,
      5,    12,   0,    0,    635,  637,  5,    144,  0,    0,    636,  633,
      1,    0,    0,    0,    636,  637,  1,    0,    0,    0,    637,  641,
      1,    0,    0,    0,    638,  639,  5,    86,   0,    0,    639,  640,
      5,    25,   0,    0,    640,  642,  3,    160,  80,   0,    641,  638,
      1,    0,    0,    0,    641,  642,  1,    0,    0,    0,    642,  937,
      1,    0,    0,    0,    643,  647,  5,    172,  0,    0,    644,  645,
      5,    12,   0,    0,    645,  646,  5,    144,  0,    0,    646,  648,
      5,    79,   0,    0,    647,  644,  1,    0,    0,    0,    647,  648,
      1,    0,    0,    0,    648,  649,  1,    0,    0,    0,    649,  650,
      3,    164,  82,   0,    650,  651,  5,    81,   0,    0,    651,  656,
      3,    162,  81,   0,    652,  653,  5,    4,    0,    0,    653,  655,
      3,    162,  81,   0,    654,  652,  1,    0,    0,    0,    655,  658,
      1,    0,    0,    0,    656,  654,  1,    0,    0,    0,    656,  657,
      1,    0,    0,    0,    657,  662,  1,    0,    0,    0,    658,  656,
      1,    0,    0,    0,    659,  660,  5,    86,   0,    0,    660,  661,
      5,    25,   0,    0,    661,  663,  3,    160,  80,   0,    662,  659,
      1,    0,    0,    0,    662,  663,  1,    0,    0,    0,    663,  937,
      1,    0,    0,    0,    664,  665,  5,    187,  0,    0,    665,  669,
      5,    174,  0,    0,    666,  670,  5,    13,   0,    0,    667,  670,
      5,    133,  0,    0,    668,  670,  3,    166,  83,   0,    669,  666,
      1,    0,    0,    0,    669,  667,  1,    0,    0,    0,    669,  668,
      1,    0,    0,    0,    670,  937,  1,    0,    0,    0,    671,  682,
      5,    85,   0,    0,    672,  677,  3,    152,  76,   0,    673,  674,
      5,    4,    0,    0,    674,  676,  3,    152,  76,   0,    675,  673,
      1,    0,    0,    0,    676,  679,  1,    0,    0,    0,    677,  675,
      1,    0,    0,    0,    677,  678,  1,    0,    0,    0,    678,  683,
      1,    0,    0,    0,    679,  677,  1,    0,    0,    0,    680,  681,
      5,    13,   0,    0,    681,  683,  5,    157,  0,    0,    682,  672,
      1,    0,    0,    0,    682,  680,  1,    0,    0,    0,    683,  684,
      1,    0,    0,    0,    684,  686,  5,    141,  0,    0,    685,  687,
      5,    198,  0,    0,    686,  685,  1,    0,    0,    0,    686,  687,
      1,    0,    0,    0,    687,  688,  1,    0,    0,    0,    688,  689,
      3,    154,  77,   0,    689,  690,  5,    206,  0,    0,    690,  694,
      3,    162,  81,   0,    691,  692,  5,    229,  0,    0,    692,  693,
      5,    85,   0,    0,    693,  695,  5,    144,  0,    0,    694,  691,
      1,    0,    0,    0,    694,  695,  1,    0,    0,    0,    695,  937,
      1,    0,    0,    0,    696,  700,  5,    172,  0,    0,    697,  698,
      5,    85,   0,    0,    698,  699,  5,    144,  0,    0,    699,  701,
      5,    79,   0,    0,    700,  697,  1,    0,    0,    0,    700,  701,
      1,    0,    0,    0,    701,  712,  1,    0,    0,    0,    702,  707,
      3,    152,  76,   0,    703,  704,  5,    4,    0,    0,    704,  706,
      3,    152,  76,   0,    705,  703,  1,    0,    0,    0,    706,  709,
      1,    0,    0,    0,    707,  705,  1,    0,    0,    0,    707,  708,
      1,    0,    0,    0,    708,  713,  1,    0,    0,    0,    709,  707,
      1,    0,    0,    0,    710,  711,  5,    13,   0,    0,    711,  713,
      5,    157,  0,    0,    712,  702,  1,    0,    0,    0,    712,  710,
      1,    0,    0,    0,    713,  714,  1,    0,    0,    0,    714,  716,
      5,    141,  0,    0,    715,  717,  5,    198,  0,    0,    716,  715,
      1,    0,    0,    0,    716,  717,  1,    0,    0,    0,    717,  718,
      1,    0,    0,    0,    718,  719,  3,    154,  77,   0,    719,  720,
      5,    81,   0,    0,    720,  721,  3,    162,  81,   0,    721,  937,
      1,    0,    0,    0,    722,  723,  5,    189,  0,    0,    723,  729,
      5,    87,   0,    0,    724,  726,  5,    141,  0,    0,    725,  727,
      5,    198,  0,    0,    726,  725,  1,    0,    0,    0,    726,  727,
      1,    0,    0,    0,    727,  728,  1,    0,    0,    0,    728,  730,
      3,    154,  77,   0,    729,  724,  1,    0,    0,    0,    729,  730,
      1,    0,    0,    0,    730,  937,  1,    0,    0,    0,    731,  733,
      5,    71,   0,    0,    732,  734,  5,    15,   0,    0,    733,  732,
      1,    0,    0,    0,    733,  734,  1,    0,    0,    0,    734,  736,
      1,    0,    0,    0,    735,  737,  5,    224,  0,    0,    736,  735,
      1,    0,    0,    0,    736,  737,  1,    0,    0,    0,    737,  749,
      1,    0,    0,    0,    738,  739,  5,    2,    0,    0,    739,  744,
      3,    144,  72,   0,    740,  741,  5,    4,    0,    0,    741,  743,
      3,    144,  72,   0,    742,  740,  1,    0,    0,    0,    743,  746,
      1,    0,    0,    0,    744,  742,  1,    0,    0,    0,    744,  745,
      1,    0,    0,    0,    745,  747,  1,    0,    0,    0,    746,  744,
      1,    0,    0,    0,    747,  748,  5,    3,    0,    0,    748,  750,
      1,    0,    0,    0,    749,  738,  1,    0,    0,    0,    749,  750,
      1,    0,    0,    0,    750,  751,  1,    0,    0,    0,    751,  937,
      3,    6,    3,    0,    752,  753,  5,    189,  0,    0,    753,  754,
      5,    38,   0,    0,    754,  755,  5,    198,  0,    0,    755,  937,
      3,    154,  77,   0,    756,  757,  5,    189,  0,    0,    757,  758,
      5,    38,   0,    0,    758,  759,  5,    226,  0,    0,    759,  937,
      3,    154,  77,   0,    760,  761,  5,    189,  0,    0,    761,  762,
      5,    38,   0,    0,    762,  763,  5,    123,  0,    0,    763,  764,
      5,    226,  0,    0,    764,  937,  3,    154,  77,   0,    765,  766,
      5,    189,  0,    0,    766,  767,  5,    38,   0,    0,    767,  768,
      5,    83,   0,    0,    768,  770,  3,    154,  77,   0,    769,  771,
      3,    122,  61,   0,    770,  769,  1,    0,    0,    0,    770,  771,
      1,    0,    0,    0,    771,  937,  1,    0,    0,    0,    772,  773,
      5,    189,  0,    0,    773,  776,  5,    199,  0,    0,    774,  775,
      7,    2,    0,    0,    775,  777,  3,    154,  77,   0,    776,  774,
      1,    0,    0,    0,    776,  777,  1,    0,    0,    0,    777,  784,
      1,    0,    0,    0,    778,  779,  5,    117,  0,    0,    779,  782,
      3,    104,  52,   0,    780,  781,  5,    64,   0,    0,    781,  783,
      3,    104,  52,   0,    782,  780,  1,    0,    0,    0,    782,  783,
      1,    0,    0,    0,    783,  785,  1,    0,    0,    0,    784,  778,
      1,    0,    0,    0,    784,  785,  1,    0,    0,    0,    785,  937,
      1,    0,    0,    0,    786,  787,  5,    189,  0,    0,    787,  790,
      5,    181,  0,    0,    788,  789,  7,    2,    0,    0,    789,  791,
      3,    166,  83,   0,    790,  788,  1,    0,    0,    0,    790,  791,
      1,    0,    0,    0,    791,  798,  1,    0,    0,    0,    792,  793,
      5,    117,  0,    0,    793,  796,  3,    104,  52,   0,    794,  795,
      5,    64,   0,    0,    795,  797,  3,    104,  52,   0,    796,  794,
      1,    0,    0,    0,    796,  797,  1,    0,    0,    0,    797,  799,
      1,    0,    0,    0,    798,  792,  1,    0,    0,    0,    798,  799,
      1,    0,    0,    0,    799,  937,  1,    0,    0,    0,    800,  801,
      5,    189,  0,    0,    801,  808,  5,    31,   0,    0,    802,  803,
      5,    117,  0,    0,    803,  806,  3,    104,  52,   0,    804,  805,
      5,    64,   0,    0,    805,  807,  3,    104,  52,   0,    806,  804,
      1,    0,    0,    0,    806,  807,  1,    0,    0,    0,    807,  809,
      1,    0,    0,    0,    808,  802,  1,    0,    0,    0,    808,  809,
      1,    0,    0,    0,    809,  937,  1,    0,    0,    0,    810,  811,
      5,    189,  0,    0,    811,  812,  5,    33,   0,    0,    812,  813,
      7,    2,    0,    0,    813,  937,  3,    154,  77,   0,    814,  815,
      5,    189,  0,    0,    815,  816,  5,    193,  0,    0,    816,  817,
      5,    79,   0,    0,    817,  937,  3,    154,  77,   0,    818,  819,
      5,    189,  0,    0,    819,  820,  5,    193,  0,    0,    820,  821,
      5,    79,   0,    0,    821,  822,  5,    2,    0,    0,    822,  823,
      3,    54,   27,   0,    823,  824,  5,    3,    0,    0,    824,  937,
      1,    0,    0,    0,    825,  827,  5,    189,  0,    0,    826,  828,
      5,    41,   0,    0,    827,  826,  1,    0,    0,    0,    827,  828,
      1,    0,    0,    0,    828,  829,  1,    0,    0,    0,    829,  832,
      5,    175,  0,    0,    830,  831,  7,    2,    0,    0,    831,  833,
      3,    166,  83,   0,    832,  830,  1,    0,    0,    0,    832,  833,
      1,    0,    0,    0,    833,  937,  1,    0,    0,    0,    834,  835,
      5,    189,  0,    0,    835,  836,  5,    174,  0,    0,    836,  839,
      5,    87,   0,    0,    837,  838,  7,    2,    0,    0,    838,  840,
      3,    166,  83,   0,    839,  837,  1,    0,    0,    0,    839,  840,
      1,    0,    0,    0,    840,  937,  1,    0,    0,    0,    841,  842,
      5,    54,   0,    0,    842,  937,  3,    154,  77,   0,    843,  844,
      5,    53,   0,    0,    844,  937,  3,    154,  77,   0,    845,  846,
      5,    189,  0,    0,    846,  853,  5,    84,   0,    0,    847,  848,
      5,    117,  0,    0,    848,  851,  3,    104,  52,   0,    849,  850,
      5,    64,   0,    0,    850,  852,  3,    104,  52,   0,    851,  849,
      1,    0,    0,    0,    851,  852,  1,    0,    0,    0,    852,  854,
      1,    0,    0,    0,    853,  847,  1,    0,    0,    0,    853,  854,
      1,    0,    0,    0,    854,  937,  1,    0,    0,    0,    855,  856,
      5,    189,  0,    0,    856,  863,  5,    186,  0,    0,    857,  858,
      5,    117,  0,    0,    858,  861,  3,    104,  52,   0,    859,  860,
      5,    64,   0,    0,    860,  862,  3,    104,  52,   0,    861,  859,
      1,    0,    0,    0,    861,  862,  1,    0,    0,    0,    862,  864,
      1,    0,    0,    0,    863,  857,  1,    0,    0,    0,    863,  864,
      1,    0,    0,    0,    864,  937,  1,    0,    0,    0,    865,  866,
      5,    187,  0,    0,    866,  867,  5,    186,  0,    0,    867,  868,
      3,    154,  77,   0,    868,  869,  5,    235,  0,    0,    869,  870,
      3,    94,   47,   0,    870,  937,  1,    0,    0,    0,    871,  872,
      5,    167,  0,    0,    872,  873,  5,    186,  0,    0,    873,  937,
      3,    154,  77,   0,    874,  875,  5,    192,  0,    0,    875,  884,
      5,    207,  0,    0,    876,  881,  3,    146,  73,   0,    877,  878,
      5,    4,    0,    0,    878,  880,  3,    146,  73,   0,    879,  877,
      1,    0,    0,    0,    880,  883,  1,    0,    0,    0,    881,  879,
      1,    0,    0,    0,    881,  882,  1,    0,    0,    0,    882,  885,
      1,    0,    0,    0,    883,  881,  1,    0,    0,    0,    884,  876,
      1,    0,    0,    0,    884,  885,  1,    0,    0,    0,    885,  937,
      1,    0,    0,    0,    886,  888,  5,    35,   0,    0,    887,  889,
      5,    231,  0,    0,    888,  887,  1,    0,    0,    0,    888,  889,
      1,    0,    0,    0,    889,  937,  1,    0,    0,    0,    890,  892,
      5,    176,  0,    0,    891,  893,  5,    231,  0,    0,    892,  891,
      1,    0,    0,    0,    892,  893,  1,    0,    0,    0,    893,  937,
      1,    0,    0,    0,    894,  895,  5,    155,  0,    0,    895,  896,
      3,    166,  83,   0,    896,  897,  5,    81,   0,    0,    897,  898,
      3,    6,    3,    0,    898,  937,  1,    0,    0,    0,    899,  900,
      5,    50,   0,    0,    900,  901,  5,    155,  0,    0,    901,  937,
      3,    166,  83,   0,    902,  903,  5,    69,   0,    0,    903,  913,
      3,    166,  83,   0,    904,  905,  5,    221,  0,    0,    905,  910,
      3,    94,   47,   0,    906,  907,  5,    4,    0,    0,    907,  909,
      3,    94,   47,   0,    908,  906,  1,    0,    0,    0,    909,  912,
      1,    0,    0,    0,    910,  908,  1,    0,    0,    0,    910,  911,
      1,    0,    0,    0,    911,  914,  1,    0,    0,    0,    912,  910,
      1,    0,    0,    0,    913,  904,  1,    0,    0,    0,    913,  914,
      1,    0,    0,    0,    914,  937,  1,    0,    0,    0,    915,  916,
      5,    54,   0,    0,    916,  917,  5,    100,  0,    0,    917,  937,
      3,    166,  83,   0,    918,  919,  5,    54,   0,    0,    919,  920,
      5,    149,  0,    0,    920,  937,  3,    166,  83,   0,    921,  922,
      5,    218,  0,    0,    922,  923,  3,    154,  77,   0,    923,  924,
      5,    187,  0,    0,    924,  929,  3,    142,  71,   0,    925,  926,
      5,    4,    0,    0,    926,  928,  3,    142,  71,   0,    927,  925,
      1,    0,    0,    0,    928,  931,  1,    0,    0,    0,    929,  927,
      1,    0,    0,    0,    929,  930,  1,    0,    0,    0,    930,  934,
      1,    0,    0,    0,    931,  929,  1,    0,    0,    0,    932,  933,
      5,    228,  0,    0,    933,  935,  3,    96,   48,   0,    934,  932,
      1,    0,    0,    0,    934,  935,  1,    0,    0,    0,    935,  937,
      1,    0,    0,    0,    936,  199,  1,    0,    0,    0,    936,  200,
      1,    0,    0,    0,    936,  202,  1,    0,    0,    0,    936,  207,
      1,    0,    0,    0,    936,  219,  1,    0,    0,    0,    936,  229,
      1,    0,    0,    0,    936,  236,  1,    0,    0,    0,    936,  270,
      1,    0,    0,    0,    936,  296,  1,    0,    0,    0,    936,  303,
      1,    0,    0,    0,    936,  311,  1,    0,    0,    0,    936,  318,
      1,    0,    0,    0,    936,  321,  1,    0,    0,    0,    936,  332,
      1,    0,    0,    0,    936,  349,  1,    0,    0,    0,    936,  364,
      1,    0,    0,    0,    936,  380,  1,    0,    0,    0,    936,  390,
      1,    0,    0,    0,    936,  405,  1,    0,    0,    0,    936,  421,
      1,    0,    0,    0,    936,  437,  1,    0,    0,    0,    936,  448,
      1,    0,    0,    0,    936,  454,  1,    0,    0,    0,    936,  472,
      1,    0,    0,    0,    936,  486,  1,    0,    0,    0,    936,  497,
      1,    0,    0,    0,    936,  504,  1,    0,    0,    0,    936,  529,
      1,    0,    0,    0,    936,  537,  1,    0,    0,    0,    936,  544,
      1,    0,    0,    0,    936,  575,  1,    0,    0,    0,    936,  583,
      1,    0,    0,    0,    936,  596,  1,    0,    0,    0,    936,  611,
      1,    0,    0,    0,    936,  619,  1,    0,    0,    0,    936,  622,
      1,    0,    0,    0,    936,  643,  1,    0,    0,    0,    936,  664,
      1,    0,    0,    0,    936,  671,  1,    0,    0,    0,    936,  696,
      1,    0,    0,    0,    936,  722,  1,    0,    0,    0,    936,  731,
      1,    0,    0,    0,    936,  752,  1,    0,    0,    0,    936,  756,
      1,    0,    0,    0,    936,  760,  1,    0,    0,    0,    936,  765,
      1,    0,    0,    0,    936,  772,  1,    0,    0,    0,    936,  786,
      1,    0,    0,    0,    936,  800,  1,    0,    0,    0,    936,  810,
      1,    0,    0,    0,    936,  814,  1,    0,    0,    0,    936,  818,
      1,    0,    0,    0,    936,  825,  1,    0,    0,    0,    936,  834,
      1,    0,    0,    0,    936,  841,  1,    0,    0,    0,    936,  843,
      1,    0,    0,    0,    936,  845,  1,    0,    0,    0,    936,  855,
      1,    0,    0,    0,    936,  865,  1,    0,    0,    0,    936,  871,
      1,    0,    0,    0,    936,  874,  1,    0,    0,    0,    936,  886,
      1,    0,    0,    0,    936,  890,  1,    0,    0,    0,    936,  894,
      1,    0,    0,    0,    936,  899,  1,    0,    0,    0,    936,  902,
      1,    0,    0,    0,    936,  915,  1,    0,    0,    0,    936,  918,
      1,    0,    0,    0,    936,  921,  1,    0,    0,    0,    937,  7,
      1,    0,    0,    0,    938,  940,  3,    10,   5,    0,    939,  938,
      1,    0,    0,    0,    939,  940,  1,    0,    0,    0,    940,  941,
      1,    0,    0,    0,    941,  942,  3,    46,   23,   0,    942,  9,
      1,    0,    0,    0,    943,  945,  5,    229,  0,    0,    944,  946,
      5,    161,  0,    0,    945,  944,  1,    0,    0,    0,    945,  946,
      1,    0,    0,    0,    946,  947,  1,    0,    0,    0,    947,  952,
      3,    64,   32,   0,    948,  949,  5,    4,    0,    0,    949,  951,
      3,    64,   32,   0,    950,  948,  1,    0,    0,    0,    951,  954,
      1,    0,    0,    0,    952,  950,  1,    0,    0,    0,    952,  953,
      1,    0,    0,    0,    953,  956,  1,    0,    0,    0,    954,  952,
      1,    0,    0,    0,    955,  957,  5,    4,    0,    0,    956,  955,
      1,    0,    0,    0,    956,  957,  1,    0,    0,    0,    957,  11,
      1,    0,    0,    0,    958,  962,  3,    170,  85,   0,    959,  962,
      3,    14,   7,    0,    960,  962,  3,    16,   8,    0,    961,  958,
      1,    0,    0,    0,    961,  959,  1,    0,    0,    0,    961,  960,
      1,    0,    0,    0,    962,  13,   1,    0,    0,    0,    963,  964,
      3,    166,  83,   0,    964,  967,  3,    124,  62,   0,    965,  966,
      5,    135,  0,    0,    966,  968,  5,    136,  0,    0,    967,  965,
      1,    0,    0,    0,    967,  968,  1,    0,    0,    0,    968,  971,
      1,    0,    0,    0,    969,  970,  5,    34,   0,    0,    970,  972,
      3,    104,  52,   0,    971,  969,  1,    0,    0,    0,    971,  972,
      1,    0,    0,    0,    972,  975,  1,    0,    0,    0,    973,  974,
      5,    229,  0,    0,    974,  976,  3,    18,   9,    0,    975,  973,
      1,    0,    0,    0,    975,  976,  1,    0,    0,    0,    976,  15,
      1,    0,    0,    0,    977,  978,  5,    117,  0,    0,    978,  981,
      3,    154,  77,   0,    979,  980,  7,    3,    0,    0,    980,  982,
      5,    158,  0,    0,    981,  979,  1,    0,    0,    0,    981,  982,
      1,    0,    0,    0,    982,  17,   1,    0,    0,    0,    983,  984,
      5,    2,    0,    0,    984,  989,  3,    20,   10,   0,    985,  986,
      5,    4,    0,    0,    986,  988,  3,    20,   10,   0,    987,  985,
      1,    0,    0,    0,    988,  991,  1,    0,    0,    0,    989,  987,
      1,    0,    0,    0,    989,  990,  1,    0,    0,    0,    990,  992,
      1,    0,    0,    0,    991,  989,  1,    0,    0,    0,    992,  993,
      5,    3,    0,    0,    993,  19,   1,    0,    0,    0,    994,  995,
      3,    166,  83,   0,    995,  996,  5,    235,  0,    0,    996,  997,
      3,    94,   47,   0,    997,  21,   1,    0,    0,    0,    998,  999,
      3,    166,  83,   0,    999,  1000, 3,    124,  62,   0,    1000, 23,
      1,    0,    0,    0,    1001, 1003, 3,    26,   13,   0,    1002, 1001,
      1,    0,    0,    0,    1003, 1006, 1,    0,    0,    0,    1004, 1002,
      1,    0,    0,    0,    1004, 1005, 1,    0,    0,    0,    1005, 25,
      1,    0,    0,    0,    1006, 1004, 1,    0,    0,    0,    1007, 1008,
      5,    112,  0,    0,    1008, 1012, 3,    38,   19,   0,    1009, 1012,
      3,    40,   20,   0,    1010, 1012, 3,    42,   21,   0,    1011, 1007,
      1,    0,    0,    0,    1011, 1009, 1,    0,    0,    0,    1011, 1010,
      1,    0,    0,    0,    1012, 27,   1,    0,    0,    0,    1013, 1015,
      3,    30,   15,   0,    1014, 1013, 1,    0,    0,    0,    1015, 1018,
      1,    0,    0,    0,    1016, 1014, 1,    0,    0,    0,    1016, 1017,
      1,    0,    0,    0,    1017, 29,   1,    0,    0,    0,    1018, 1016,
      1,    0,    0,    0,    1019, 1020, 3,    42,   21,   0,    1020, 31,
      1,    0,    0,    0,    1021, 1024, 3,    34,   17,   0,    1022, 1024,
      3,    36,   18,   0,    1023, 1021, 1,    0,    0,    0,    1023, 1022,
      1,    0,    0,    0,    1024, 33,   1,    0,    0,    0,    1025, 1026,
      5,    170,  0,    0,    1026, 1027, 3,    94,   47,   0,    1027, 35,
      1,    0,    0,    0,    1028, 1031, 5,    73,   0,    0,    1029, 1030,
      5,    126,  0,    0,    1030, 1032, 3,    44,   22,   0,    1031, 1029,
      1,    0,    0,    0,    1031, 1032, 1,    0,    0,    0,    1032, 37,
      1,    0,    0,    0,    1033, 1036, 5,    191,  0,    0,    1034, 1036,
      3,    166,  83,   0,    1035, 1033, 1,    0,    0,    0,    1035, 1034,
      1,    0,    0,    0,    1036, 39,   1,    0,    0,    0,    1037, 1041,
      5,    55,   0,    0,    1038, 1039, 5,    135,  0,    0,    1039, 1041,
      5,    55,   0,    0,    1040, 1037, 1,    0,    0,    0,    1040, 1038,
      1,    0,    0,    0,    1041, 41,   1,    0,    0,    0,    1042, 1043,
      5,    171,  0,    0,    1043, 1044, 5,    136,  0,    0,    1044, 1045,
      5,    141,  0,    0,    1045, 1046, 5,    136,  0,    0,    1046, 1052,
      5,    100,  0,    0,    1047, 1048, 5,    27,   0,    0,    1048, 1049,
      5,    141,  0,    0,    1049, 1050, 5,    136,  0,    0,    1050, 1052,
      5,    100,  0,    0,    1051, 1042, 1,    0,    0,    0,    1051, 1047,
      1,    0,    0,    0,    1052, 43,   1,    0,    0,    0,    1053, 1054,
      3,    166,  83,   0,    1054, 45,   1,    0,    0,    0,    1055, 1066,
      3,    48,   24,   0,    1056, 1057, 5,    146,  0,    0,    1057, 1058,
      5,    25,   0,    0,    1058, 1063, 3,    52,   26,   0,    1059, 1060,
      5,    4,    0,    0,    1060, 1062, 3,    52,   26,   0,    1061, 1059,
      1,    0,    0,    0,    1062, 1065, 1,    0,    0,    0,    1063, 1061,
      1,    0,    0,    0,    1063, 1064, 1,    0,    0,    0,    1064, 1067,
      1,    0,    0,    0,    1065, 1063, 1,    0,    0,    0,    1066, 1056,
      1,    0,    0,    0,    1066, 1067, 1,    0,    0,    0,    1067, 1073,
      1,    0,    0,    0,    1068, 1069, 5,    140,  0,    0,    1069, 1071,
      5,    250,  0,    0,    1070, 1072, 7,    4,    0,    0,    1071, 1070,
      1,    0,    0,    0,    1071, 1072, 1,    0,    0,    0,    1072, 1074,
      1,    0,    0,    0,    1073, 1068, 1,    0,    0,    0,    1073, 1074,
      1,    0,    0,    0,    1074, 1084, 1,    0,    0,    0,    1075, 1076,
      5,    118,  0,    0,    1076, 1083, 7,    5,    0,    0,    1077, 1078,
      5,    75,   0,    0,    1078, 1079, 5,    77,   0,    0,    1079, 1080,
      5,    250,  0,    0,    1080, 1081, 5,    179,  0,    0,    1081, 1083,
      5,    142,  0,    0,    1082, 1075, 1,    0,    0,    0,    1082, 1077,
      1,    0,    0,    0,    1083, 1085, 1,    0,    0,    0,    1084, 1082,
      1,    0,    0,    0,    1084, 1085, 1,    0,    0,    0,    1085, 47,
      1,    0,    0,    0,    1086, 1087, 6,    24,   -1,   0,    1087, 1088,
      3,    50,   25,   0,    1088, 1103, 1,    0,    0,    0,    1089, 1090,
      10,   2,    0,    0,    1090, 1092, 5,    102,  0,    0,    1091, 1093,
      3,    66,   33,   0,    1092, 1091, 1,    0,    0,    0,    1092, 1093,
      1,    0,    0,    0,    1093, 1094, 1,    0,    0,    0,    1094, 1102,
      3,    48,   24,   3,    1095, 1096, 10,   1,    0,    0,    1096, 1098,
      7,    6,    0,    0,    1097, 1099, 3,    66,   33,   0,    1098, 1097,
      1,    0,    0,    0,    1098, 1099, 1,    0,    0,    0,    1099, 1100,
      1,    0,    0,    0,    1100, 1102, 3,    48,   24,   2,    1101, 1089,
      1,    0,    0,    0,    1101, 1095, 1,    0,    0,    0,    1102, 1105,
      1,    0,    0,    0,    1103, 1101, 1,    0,    0,    0,    1103, 1104,
      1,    0,    0,    0,    1104, 49,   1,    0,    0,    0,    1105, 1103,
      1,    0,    0,    0,    1106, 1126, 3,    54,   27,   0,    1107, 1108,
      5,    198,  0,    0,    1108, 1126, 3,    154,  77,   0,    1109, 1110,
      5,    223,  0,    0,    1110, 1115, 3,    94,   47,   0,    1111, 1112,
      5,    4,    0,    0,    1112, 1114, 3,    94,   47,   0,    1113, 1111,
      1,    0,    0,    0,    1114, 1117, 1,    0,    0,    0,    1115, 1113,
      1,    0,    0,    0,    1115, 1116, 1,    0,    0,    0,    1116, 1119,
      1,    0,    0,    0,    1117, 1115, 1,    0,    0,    0,    1118, 1120,
      5,    4,    0,    0,    1119, 1118, 1,    0,    0,    0,    1119, 1120,
      1,    0,    0,    0,    1120, 1126, 1,    0,    0,    0,    1121, 1122,
      5,    2,    0,    0,    1122, 1123, 3,    46,   23,   0,    1123, 1124,
      5,    3,    0,    0,    1124, 1126, 1,    0,    0,    0,    1125, 1106,
      1,    0,    0,    0,    1125, 1107, 1,    0,    0,    0,    1125, 1109,
      1,    0,    0,    0,    1125, 1121, 1,    0,    0,    0,    1126, 51,
      1,    0,    0,    0,    1127, 1129, 3,    94,   47,   0,    1128, 1130,
      7,    7,    0,    0,    1129, 1128, 1,    0,    0,    0,    1129, 1130,
      1,    0,    0,    0,    1130, 1133, 1,    0,    0,    0,    1131, 1132,
      5,    138,  0,    0,    1132, 1134, 7,    8,    0,    0,    1133, 1131,
      1,    0,    0,    0,    1133, 1134, 1,    0,    0,    0,    1134, 53,
      1,    0,    0,    0,    1135, 1137, 5,    184,  0,    0,    1136, 1138,
      3,    66,   33,   0,    1137, 1136, 1,    0,    0,    0,    1137, 1138,
      1,    0,    0,    0,    1138, 1139, 1,    0,    0,    0,    1139, 1144,
      3,    68,   34,   0,    1140, 1141, 5,    4,    0,    0,    1141, 1143,
      3,    68,   34,   0,    1142, 1140, 1,    0,    0,    0,    1143, 1146,
      1,    0,    0,    0,    1144, 1142, 1,    0,    0,    0,    1144, 1145,
      1,    0,    0,    0,    1145, 1148, 1,    0,    0,    0,    1146, 1144,
      1,    0,    0,    0,    1147, 1149, 5,    4,    0,    0,    1148, 1147,
      1,    0,    0,    0,    1148, 1149, 1,    0,    0,    0,    1149, 1159,
      1,    0,    0,    0,    1150, 1151, 5,    81,   0,    0,    1151, 1156,
      3,    78,   39,   0,    1152, 1153, 5,    4,    0,    0,    1153, 1155,
      3,    78,   39,   0,    1154, 1152, 1,    0,    0,    0,    1155, 1158,
      1,    0,    0,    0,    1156, 1154, 1,    0,    0,    0,    1156, 1157,
      1,    0,    0,    0,    1157, 1160, 1,    0,    0,    0,    1158, 1156,
      1,    0,    0,    0,    1159, 1150, 1,    0,    0,    0,    1159, 1160,
      1,    0,    0,    0,    1160, 1163, 1,    0,    0,    0,    1161, 1162,
      5,    228,  0,    0,    1162, 1164, 3,    96,   48,   0,    1163, 1161,
      1,    0,    0,    0,    1163, 1164, 1,    0,    0,    0,    1164, 1168,
      1,    0,    0,    0,    1165, 1166, 5,    90,   0,    0,    1166, 1167,
      5,    25,   0,    0,    1167, 1169, 3,    58,   29,   0,    1168, 1165,
      1,    0,    0,    0,    1168, 1169, 1,    0,    0,    0,    1169, 1172,
      1,    0,    0,    0,    1170, 1171, 5,    93,   0,    0,    1171, 1173,
      3,    96,   48,   0,    1172, 1170, 1,    0,    0,    0,    1172, 1173,
      1,    0,    0,    0,    1173, 1183, 1,    0,    0,    0,    1174, 1175,
      5,    230,  0,    0,    1175, 1180, 3,    56,   28,   0,    1176, 1177,
      5,    4,    0,    0,    1177, 1179, 3,    56,   28,   0,    1178, 1176,
      1,    0,    0,    0,    1179, 1182, 1,    0,    0,    0,    1180, 1178,
      1,    0,    0,    0,    1180, 1181, 1,    0,    0,    0,    1181, 1184,
      1,    0,    0,    0,    1182, 1180, 1,    0,    0,    0,    1183, 1174,
      1,    0,    0,    0,    1183, 1184, 1,    0,    0,    0,    1184, 1219,
      1,    0,    0,    0,    1185, 1186, 5,    81,   0,    0,    1186, 1191,
      3,    78,   39,   0,    1187, 1188, 5,    4,    0,    0,    1188, 1190,
      3,    78,   39,   0,    1189, 1187, 1,    0,    0,    0,    1190, 1193,
      1,    0,    0,    0,    1191, 1189, 1,    0,    0,    0,    1191, 1192,
      1,    0,    0,    0,    1192, 1196, 1,    0,    0,    0,    1193, 1191,
      1,    0,    0,    0,    1194, 1195, 5,    228,  0,    0,    1195, 1197,
      3,    96,   48,   0,    1196, 1194, 1,    0,    0,    0,    1196, 1197,
      1,    0,    0,    0,    1197, 1201, 1,    0,    0,    0,    1198, 1199,
      5,    90,   0,    0,    1199, 1200, 5,    25,   0,    0,    1200, 1202,
      3,    58,   29,   0,    1201, 1198, 1,    0,    0,    0,    1201, 1202,
      1,    0,    0,    0,    1202, 1205, 1,    0,    0,    0,    1203, 1204,
      5,    93,   0,    0,    1204, 1206, 3,    96,   48,   0,    1205, 1203,
      1,    0,    0,    0,    1205, 1206, 1,    0,    0,    0,    1206, 1216,
      1,    0,    0,    0,    1207, 1208, 5,    230,  0,    0,    1208, 1213,
      3,    56,   28,   0,    1209, 1210, 5,    4,    0,    0,    1210, 1212,
      3,    56,   28,   0,    1211, 1209, 1,    0,    0,    0,    1212, 1215,
      1,    0,    0,    0,    1213, 1211, 1,    0,    0,    0,    1213, 1214,
      1,    0,    0,    0,    1214, 1217, 1,    0,    0,    0,    1215, 1213,
      1,    0,    0,    0,    1216, 1207, 1,    0,    0,    0,    1216, 1217,
      1,    0,    0,    0,    1217, 1219, 1,    0,    0,    0,    1218, 1135,
      1,    0,    0,    0,    1218, 1185, 1,    0,    0,    0,    1219, 55,
      1,    0,    0,    0,    1220, 1221, 3,    166,  83,   0,    1221, 1222,
      5,    19,   0,    0,    1222, 1223, 5,    2,    0,    0,    1223, 1224,
      3,    136,  68,   0,    1224, 1225, 5,    3,    0,    0,    1225, 57,
      1,    0,    0,    0,    1226, 1228, 3,    66,   33,   0,    1227, 1226,
      1,    0,    0,    0,    1227, 1228, 1,    0,    0,    0,    1228, 1229,
      1,    0,    0,    0,    1229, 1234, 3,    60,   30,   0,    1230, 1231,
      5,    4,    0,    0,    1231, 1233, 3,    60,   30,   0,    1232, 1230,
      1,    0,    0,    0,    1233, 1236, 1,    0,    0,    0,    1234, 1232,
      1,    0,    0,    0,    1234, 1235, 1,    0,    0,    0,    1235, 59,
      1,    0,    0,    0,    1236, 1234, 1,    0,    0,    0,    1237, 1278,
      3,    62,   31,   0,    1238, 1239, 5,    177,  0,    0,    1239, 1248,
      5,    2,    0,    0,    1240, 1245, 3,    94,   47,   0,    1241, 1242,
      5,    4,    0,    0,    1242, 1244, 3,    94,   47,   0,    1243, 1241,
      1,    0,    0,    0,    1244, 1247, 1,    0,    0,    0,    1245, 1243,
      1,    0,    0,    0,    1245, 1246, 1,    0,    0,    0,    1246, 1249,
      1,    0,    0,    0,    1247, 1245, 1,    0,    0,    0,    1248, 1240,
      1,    0,    0,    0,    1248, 1249, 1,    0,    0,    0,    1249, 1250,
      1,    0,    0,    0,    1250, 1278, 5,    3,    0,    0,    1251, 1252,
      5,    40,   0,    0,    1252, 1261, 5,    2,    0,    0,    1253, 1258,
      3,    94,   47,   0,    1254, 1255, 5,    4,    0,    0,    1255, 1257,
      3,    94,   47,   0,    1256, 1254, 1,    0,    0,    0,    1257, 1260,
      1,    0,    0,    0,    1258, 1256, 1,    0,    0,    0,    1258, 1259,
      1,    0,    0,    0,    1259, 1262, 1,    0,    0,    0,    1260, 1258,
      1,    0,    0,    0,    1261, 1253, 1,    0,    0,    0,    1261, 1262,
      1,    0,    0,    0,    1262, 1263, 1,    0,    0,    0,    1263, 1278,
      5,    3,    0,    0,    1264, 1265, 5,    91,   0,    0,    1265, 1266,
      5,    188,  0,    0,    1266, 1267, 5,    2,    0,    0,    1267, 1272,
      3,    62,   31,   0,    1268, 1269, 5,    4,    0,    0,    1269, 1271,
      3,    62,   31,   0,    1270, 1268, 1,    0,    0,    0,    1271, 1274,
      1,    0,    0,    0,    1272, 1270, 1,    0,    0,    0,    1272, 1273,
      1,    0,    0,    0,    1273, 1275, 1,    0,    0,    0,    1274, 1272,
      1,    0,    0,    0,    1275, 1276, 5,    3,    0,    0,    1276, 1278,
      1,    0,    0,    0,    1277, 1237, 1,    0,    0,    0,    1277, 1238,
      1,    0,    0,    0,    1277, 1251, 1,    0,    0,    0,    1277, 1264,
      1,    0,    0,    0,    1278, 61,   1,    0,    0,    0,    1279, 1288,
      5,    2,    0,    0,    1280, 1285, 3,    94,   47,   0,    1281, 1282,
      5,    4,    0,    0,    1282, 1284, 3,    94,   47,   0,    1283, 1281,
      1,    0,    0,    0,    1284, 1287, 1,    0,    0,    0,    1285, 1283,
      1,    0,    0,    0,    1285, 1286, 1,    0,    0,    0,    1286, 1289,
      1,    0,    0,    0,    1287, 1285, 1,    0,    0,    0,    1288, 1280,
      1,    0,    0,    0,    1288, 1289, 1,    0,    0,    0,    1289, 1290,
      1,    0,    0,    0,    1290, 1293, 5,    3,    0,    0,    1291, 1293,
      3,    94,   47,   0,    1292, 1279, 1,    0,    0,    0,    1292, 1291,
      1,    0,    0,    0,    1293, 63,   1,    0,    0,    0,    1294, 1296,
      3,    166,  83,   0,    1295, 1297, 3,    90,   45,   0,    1296, 1295,
      1,    0,    0,    0,    1296, 1297, 1,    0,    0,    0,    1297, 1298,
      1,    0,    0,    0,    1298, 1299, 5,    19,   0,    0,    1299, 1300,
      5,    2,    0,    0,    1300, 1301, 3,    8,    4,    0,    1301, 1302,
      5,    3,    0,    0,    1302, 65,   1,    0,    0,    0,    1303, 1304,
      7,    9,    0,    0,    1304, 67,   1,    0,    0,    0,    1305, 1306,
      3,    154,  77,   0,    1306, 1307, 5,    1,    0,    0,    1307, 1309,
      5,    243,  0,    0,    1308, 1310, 3,    70,   35,   0,    1309, 1308,
      1,    0,    0,    0,    1309, 1310, 1,    0,    0,    0,    1310, 1339,
      1,    0,    0,    0,    1311, 1313, 5,    243,  0,    0,    1312, 1314,
      3,    70,   35,   0,    1313, 1312, 1,    0,    0,    0,    1313, 1314,
      1,    0,    0,    0,    1314, 1339, 1,    0,    0,    0,    1315, 1316,
      3,    154,  77,   0,    1316, 1317, 5,    1,    0,    0,    1317, 1318,
      5,    33,   0,    0,    1318, 1319, 5,    2,    0,    0,    1319, 1320,
      5,    247,  0,    0,    1320, 1322, 5,    3,    0,    0,    1321, 1323,
      3,    70,   35,   0,    1322, 1321, 1,    0,    0,    0,    1322, 1323,
      1,    0,    0,    0,    1323, 1339, 1,    0,    0,    0,    1324, 1325,
      5,    33,   0,    0,    1325, 1326, 5,    2,    0,    0,    1326, 1327,
      5,    247,  0,    0,    1327, 1329, 5,    3,    0,    0,    1328, 1330,
      3,    70,   35,   0,    1329, 1328, 1,    0,    0,    0,    1329, 1330,
      1,    0,    0,    0,    1330, 1339, 1,    0,    0,    0,    1331, 1336,
      3,    94,   47,   0,    1332, 1334, 5,    19,   0,    0,    1333, 1332,
      1,    0,    0,    0,    1333, 1334, 1,    0,    0,    0,    1334, 1335,
      1,    0,    0,    0,    1335, 1337, 3,    166,  83,   0,    1336, 1333,
      1,    0,    0,    0,    1336, 1337, 1,    0,    0,    0,    1337, 1339,
      1,    0,    0,    0,    1338, 1305, 1,    0,    0,    0,    1338, 1311,
      1,    0,    0,    0,    1338, 1315, 1,    0,    0,    0,    1338, 1324,
      1,    0,    0,    0,    1338, 1331, 1,    0,    0,    0,    1339, 69,
      1,    0,    0,    0,    1340, 1343, 3,    72,   36,   0,    1341, 1343,
      3,    74,   37,   0,    1342, 1340, 1,    0,    0,    0,    1342, 1341,
      1,    0,    0,    0,    1343, 1344, 1,    0,    0,    0,    1344, 1342,
      1,    0,    0,    0,    1344, 1345, 1,    0,    0,    0,    1345, 71,
      1,    0,    0,    0,    1346, 1347, 5,    66,   0,    0,    1347, 1348,
      5,    2,    0,    0,    1348, 1353, 3,    166,  83,   0,    1349, 1350,
      5,    4,    0,    0,    1350, 1352, 3,    166,  83,   0,    1351, 1349,
      1,    0,    0,    0,    1352, 1355, 1,    0,    0,    0,    1353, 1351,
      1,    0,    0,    0,    1353, 1354, 1,    0,    0,    0,    1354, 1356,
      1,    0,    0,    0,    1355, 1353, 1,    0,    0,    0,    1356, 1357,
      5,    3,    0,    0,    1357, 73,   1,    0,    0,    0,    1358, 1359,
      5,    166,  0,    0,    1359, 1360, 5,    2,    0,    0,    1360, 1365,
      3,    76,   38,   0,    1361, 1362, 5,    4,    0,    0,    1362, 1364,
      3,    76,   38,   0,    1363, 1361, 1,    0,    0,    0,    1364, 1367,
      1,    0,    0,    0,    1365, 1363, 1,    0,    0,    0,    1365, 1366,
      1,    0,    0,    0,    1366, 1368, 1,    0,    0,    0,    1367, 1365,
      1,    0,    0,    0,    1368, 1369, 5,    3,    0,    0,    1369, 75,
      1,    0,    0,    0,    1370, 1371, 3,    94,   47,   0,    1371, 1372,
      5,    19,   0,    0,    1372, 1373, 3,    166,  83,   0,    1373, 77,
      1,    0,    0,    0,    1374, 1375, 6,    39,   -1,   0,    1375, 1376,
      3,    84,   42,   0,    1376, 1395, 1,    0,    0,    0,    1377, 1391,
      10,   2,    0,    0,    1378, 1379, 5,    39,   0,    0,    1379, 1380,
      5,    110,  0,    0,    1380, 1392, 3,    84,   42,   0,    1381, 1382,
      3,    80,   40,   0,    1382, 1383, 5,    110,  0,    0,    1383, 1384,
      3,    78,   39,   0,    1384, 1385, 3,    82,   41,   0,    1385, 1392,
      1,    0,    0,    0,    1386, 1387, 5,    127,  0,    0,    1387, 1388,
      3,    80,   40,   0,    1388, 1389, 5,    110,  0,    0,    1389, 1390,
      3,    84,   42,   0,    1390, 1392, 1,    0,    0,    0,    1391, 1378,
      1,    0,    0,    0,    1391, 1381, 1,    0,    0,    0,    1391, 1386,
      1,    0,    0,    0,    1392, 1394, 1,    0,    0,    0,    1393, 1377,
      1,    0,    0,    0,    1394, 1397, 1,    0,    0,    0,    1395, 1393,
      1,    0,    0,    0,    1395, 1396, 1,    0,    0,    0,    1396, 79,
      1,    0,    0,    0,    1397, 1395, 1,    0,    0,    0,    1398, 1400,
      5,    99,   0,    0,    1399, 1398, 1,    0,    0,    0,    1399, 1400,
      1,    0,    0,    0,    1400, 1414, 1,    0,    0,    0,    1401, 1403,
      5,    115,  0,    0,    1402, 1404, 5,    148,  0,    0,    1403, 1402,
      1,    0,    0,    0,    1403, 1404, 1,    0,    0,    0,    1404, 1414,
      1,    0,    0,    0,    1405, 1407, 5,    173,  0,    0,    1406, 1408,
      5,    148,  0,    0,    1407, 1406, 1,    0,    0,    0,    1407, 1408,
      1,    0,    0,    0,    1408, 1414, 1,    0,    0,    0,    1409, 1411,
      5,    82,   0,    0,    1410, 1412, 5,    148,  0,    0,    1411, 1410,
      1,    0,    0,    0,    1411, 1412, 1,    0,    0,    0,    1412, 1414,
      1,    0,    0,    0,    1413, 1399, 1,    0,    0,    0,    1413, 1401,
      1,    0,    0,    0,    1413, 1405, 1,    0,    0,    0,    1413, 1409,
      1,    0,    0,    0,    1414, 81,   1,    0,    0,    0,    1415, 1416,
      5,    141,  0,    0,    1416, 1430, 3,    96,   48,   0,    1417, 1418,
      5,    221,  0,    0,    1418, 1419, 5,    2,    0,    0,    1419, 1424,
      3,    166,  83,   0,    1420, 1421, 5,    4,    0,    0,    1421, 1423,
      3,    166,  83,   0,    1422, 1420, 1,    0,    0,    0,    1423, 1426,
      1,    0,    0,    0,    1424, 1422, 1,    0,    0,    0,    1424, 1425,
      1,    0,    0,    0,    1425, 1427, 1,    0,    0,    0,    1426, 1424,
      1,    0,    0,    0,    1427, 1428, 5,    3,    0,    0,    1428, 1430,
      1,    0,    0,    0,    1429, 1415, 1,    0,    0,    0,    1429, 1417,
      1,    0,    0,    0,    1430, 83,   1,    0,    0,    0,    1431, 1438,
      3,    88,   44,   0,    1432, 1433, 5,    200,  0,    0,    1433, 1434,
      3,    86,   43,   0,    1434, 1435, 5,    2,    0,    0,    1435, 1436,
      3,    94,   47,   0,    1436, 1437, 5,    3,    0,    0,    1437, 1439,
      1,    0,    0,    0,    1438, 1432, 1,    0,    0,    0,    1438, 1439,
      1,    0,    0,    0,    1439, 85,   1,    0,    0,    0,    1440, 1441,
      7,    10,   0,    0,    1441, 87,   1,    0,    0,    0,    1442, 1450,
      3,    92,   46,   0,    1443, 1445, 5,    19,   0,    0,    1444, 1443,
      1,    0,    0,    0,    1444, 1445, 1,    0,    0,    0,    1445, 1446,
      1,    0,    0,    0,    1446, 1448, 3,    166,  83,   0,    1447, 1449,
      3,    90,   45,   0,    1448, 1447, 1,    0,    0,    0,    1448, 1449,
      1,    0,    0,    0,    1449, 1451, 1,    0,    0,    0,    1450, 1444,
      1,    0,    0,    0,    1450, 1451, 1,    0,    0,    0,    1451, 89,
      1,    0,    0,    0,    1452, 1453, 5,    2,    0,    0,    1453, 1458,
      3,    166,  83,   0,    1454, 1455, 5,    4,    0,    0,    1455, 1457,
      3,    166,  83,   0,    1456, 1454, 1,    0,    0,    0,    1457, 1460,
      1,    0,    0,    0,    1458, 1456, 1,    0,    0,    0,    1458, 1459,
      1,    0,    0,    0,    1459, 1461, 1,    0,    0,    0,    1460, 1458,
      1,    0,    0,    0,    1461, 1462, 5,    3,    0,    0,    1462, 91,
      1,    0,    0,    0,    1463, 1465, 3,    154,  77,   0,    1464, 1466,
      3,    156,  78,   0,    1465, 1464, 1,    0,    0,    0,    1465, 1466,
      1,    0,    0,    0,    1466, 1496, 1,    0,    0,    0,    1467, 1468,
      5,    2,    0,    0,    1468, 1469, 3,    8,    4,    0,    1469, 1470,
      5,    3,    0,    0,    1470, 1496, 1,    0,    0,    0,    1471, 1472,
      5,    217,  0,    0,    1472, 1473, 5,    2,    0,    0,    1473, 1478,
      3,    94,   47,   0,    1474, 1475, 5,    4,    0,    0,    1475, 1477,
      3,    94,   47,   0,    1476, 1474, 1,    0,    0,    0,    1477, 1480,
      1,    0,    0,    0,    1478, 1476, 1,    0,    0,    0,    1478, 1479,
      1,    0,    0,    0,    1479, 1481, 1,    0,    0,    0,    1480, 1478,
      1,    0,    0,    0,    1481, 1484, 5,    3,    0,    0,    1482, 1483,
      5,    229,  0,    0,    1483, 1485, 5,    147,  0,    0,    1484, 1482,
      1,    0,    0,    0,    1484, 1485, 1,    0,    0,    0,    1485, 1496,
      1,    0,    0,    0,    1486, 1487, 5,    114,  0,    0,    1487, 1488,
      5,    2,    0,    0,    1488, 1489, 3,    8,    4,    0,    1489, 1490,
      5,    3,    0,    0,    1490, 1496, 1,    0,    0,    0,    1491, 1492,
      5,    2,    0,    0,    1492, 1493, 3,    78,   39,   0,    1493, 1494,
      5,    3,    0,    0,    1494, 1496, 1,    0,    0,    0,    1495, 1463,
      1,    0,    0,    0,    1495, 1467, 1,    0,    0,    0,    1495, 1471,
      1,    0,    0,    0,    1495, 1486, 1,    0,    0,    0,    1495, 1491,
      1,    0,    0,    0,    1496, 93,   1,    0,    0,    0,    1497, 1498,
      3,    96,   48,   0,    1498, 95,   1,    0,    0,    0,    1499, 1500,
      6,    48,   -1,   0,    1500, 1502, 3,    100,  50,   0,    1501, 1503,
      3,    98,   49,   0,    1502, 1501, 1,    0,    0,    0,    1502, 1503,
      1,    0,    0,    0,    1503, 1507, 1,    0,    0,    0,    1504, 1505,
      5,    135,  0,    0,    1505, 1507, 3,    96,   48,   3,    1506, 1499,
      1,    0,    0,    0,    1506, 1504, 1,    0,    0,    0,    1507, 1516,
      1,    0,    0,    0,    1508, 1509, 10,   2,    0,    0,    1509, 1510,
      5,    16,   0,    0,    1510, 1515, 3,    96,   48,   3,    1511, 1512,
      10,   1,    0,    0,    1512, 1513, 5,    145,  0,    0,    1513, 1515,
      3,    96,   48,   2,    1514, 1508, 1,    0,    0,    0,    1514, 1511,
      1,    0,    0,    0,    1515, 1518, 1,    0,    0,    0,    1516, 1514,
      1,    0,    0,    0,    1516, 1517, 1,    0,    0,    0,    1517, 97,
      1,    0,    0,    0,    1518, 1516, 1,    0,    0,    0,    1519, 1520,
      3,    110,  55,   0,    1520, 1521, 3,    100,  50,   0,    1521, 1581,
      1,    0,    0,    0,    1522, 1523, 3,    110,  55,   0,    1523, 1524,
      3,    112,  56,   0,    1524, 1525, 5,    2,    0,    0,    1525, 1526,
      3,    8,    4,    0,    1526, 1527, 5,    3,    0,    0,    1527, 1581,
      1,    0,    0,    0,    1528, 1530, 5,    135,  0,    0,    1529, 1528,
      1,    0,    0,    0,    1529, 1530, 1,    0,    0,    0,    1530, 1531,
      1,    0,    0,    0,    1531, 1532, 5,    24,   0,    0,    1532, 1533,
      3,    100,  50,   0,    1533, 1534, 5,    16,   0,    0,    1534, 1535,
      3,    100,  50,   0,    1535, 1581, 1,    0,    0,    0,    1536, 1538,
      5,    135,  0,    0,    1537, 1536, 1,    0,    0,    0,    1537, 1538,
      1,    0,    0,    0,    1538, 1539, 1,    0,    0,    0,    1539, 1540,
      5,    97,   0,    0,    1540, 1541, 5,    2,    0,    0,    1541, 1546,
      3,    94,   47,   0,    1542, 1543, 5,    4,    0,    0,    1543, 1545,
      3,    94,   47,   0,    1544, 1542, 1,    0,    0,    0,    1545, 1548,
      1,    0,    0,    0,    1546, 1544, 1,    0,    0,    0,    1546, 1547,
      1,    0,    0,    0,    1547, 1549, 1,    0,    0,    0,    1548, 1546,
      1,    0,    0,    0,    1549, 1550, 5,    3,    0,    0,    1550, 1581,
      1,    0,    0,    0,    1551, 1553, 5,    135,  0,    0,    1552, 1551,
      1,    0,    0,    0,    1552, 1553, 1,    0,    0,    0,    1553, 1554,
      1,    0,    0,    0,    1554, 1555, 5,    97,   0,    0,    1555, 1556,
      5,    2,    0,    0,    1556, 1557, 3,    8,    4,    0,    1557, 1558,
      5,    3,    0,    0,    1558, 1581, 1,    0,    0,    0,    1559, 1561,
      5,    135,  0,    0,    1560, 1559, 1,    0,    0,    0,    1560, 1561,
      1,    0,    0,    0,    1561, 1562, 1,    0,    0,    0,    1562, 1563,
      5,    117,  0,    0,    1563, 1566, 3,    100,  50,   0,    1564, 1565,
      5,    64,   0,    0,    1565, 1567, 3,    100,  50,   0,    1566, 1564,
      1,    0,    0,    0,    1566, 1567, 1,    0,    0,    0,    1567, 1581,
      1,    0,    0,    0,    1568, 1570, 5,    107,  0,    0,    1569, 1571,
      5,    135,  0,    0,    1570, 1569, 1,    0,    0,    0,    1570, 1571,
      1,    0,    0,    0,    1571, 1572, 1,    0,    0,    0,    1572, 1581,
      5,    136,  0,    0,    1573, 1575, 5,    107,  0,    0,    1574, 1576,
      5,    135,  0,    0,    1575, 1574, 1,    0,    0,    0,    1575, 1576,
      1,    0,    0,    0,    1576, 1577, 1,    0,    0,    0,    1577, 1578,
      5,    57,   0,    0,    1578, 1579, 5,    81,   0,    0,    1579, 1581,
      3,    100,  50,   0,    1580, 1519, 1,    0,    0,    0,    1580, 1522,
      1,    0,    0,    0,    1580, 1529, 1,    0,    0,    0,    1580, 1537,
      1,    0,    0,    0,    1580, 1552, 1,    0,    0,    0,    1580, 1560,
      1,    0,    0,    0,    1580, 1568, 1,    0,    0,    0,    1580, 1573,
      1,    0,    0,    0,    1581, 99,   1,    0,    0,    0,    1582, 1583,
      6,    50,   -1,   0,    1583, 1587, 3,    102,  51,   0,    1584, 1585,
      7,    11,   0,    0,    1585, 1587, 3,    100,  50,   4,    1586, 1582,
      1,    0,    0,    0,    1586, 1584, 1,    0,    0,    0,    1587, 1602,
      1,    0,    0,    0,    1588, 1589, 10,   3,    0,    0,    1589, 1590,
      7,    12,   0,    0,    1590, 1601, 3,    100,  50,   4,    1591, 1592,
      10,   2,    0,    0,    1592, 1593, 7,    11,   0,    0,    1593, 1601,
      3,    100,  50,   3,    1594, 1595, 10,   1,    0,    0,    1595, 1596,
      5,    246,  0,    0,    1596, 1601, 3,    100,  50,   2,    1597, 1598,
      10,   5,    0,    0,    1598, 1599, 5,    21,   0,    0,    1599, 1601,
      3,    108,  54,   0,    1600, 1588, 1,    0,    0,    0,    1600, 1591,
      1,    0,    0,    0,    1600, 1594, 1,    0,    0,    0,    1600, 1597,
      1,    0,    0,    0,    1601, 1604, 1,    0,    0,    0,    1602, 1600,
      1,    0,    0,    0,    1602, 1603, 1,    0,    0,    0,    1603, 101,
      1,    0,    0,    0,    1604, 1602, 1,    0,    0,    0,    1605, 1606,
      6,    51,   -1,   0,    1606, 1862, 5,    136,  0,    0,    1607, 1862,
      3,    116,  58,   0,    1608, 1609, 3,    124,  62,   0,    1609, 1610,
      3,    104,  52,   0,    1610, 1862, 1,    0,    0,    0,    1611, 1612,
      5,    259,  0,    0,    1612, 1862, 3,    104,  52,   0,    1613, 1862,
      3,    168,  84,   0,    1614, 1862, 3,    114,  57,   0,    1615, 1862,
      3,    104,  52,   0,    1616, 1862, 5,    249,  0,    0,    1617, 1862,
      5,    5,    0,    0,    1618, 1619, 5,    153,  0,    0,    1619, 1620,
      5,    2,    0,    0,    1620, 1621, 3,    100,  50,   0,    1621, 1622,
      5,    97,   0,    0,    1622, 1623, 3,    100,  50,   0,    1623, 1624,
      5,    3,    0,    0,    1624, 1862, 1,    0,    0,    0,    1625, 1626,
      5,    2,    0,    0,    1626, 1629, 3,    94,   47,   0,    1627, 1628,
      5,    4,    0,    0,    1628, 1630, 3,    94,   47,   0,    1629, 1627,
      1,    0,    0,    0,    1630, 1631, 1,    0,    0,    0,    1631, 1629,
      1,    0,    0,    0,    1631, 1632, 1,    0,    0,    0,    1632, 1633,
      1,    0,    0,    0,    1633, 1634, 5,    3,    0,    0,    1634, 1862,
      1,    0,    0,    0,    1635, 1636, 5,    178,  0,    0,    1636, 1637,
      5,    2,    0,    0,    1637, 1642, 3,    94,   47,   0,    1638, 1639,
      5,    4,    0,    0,    1639, 1641, 3,    94,   47,   0,    1640, 1638,
      1,    0,    0,    0,    1641, 1644, 1,    0,    0,    0,    1642, 1640,
      1,    0,    0,    0,    1642, 1643, 1,    0,    0,    0,    1643, 1645,
      1,    0,    0,    0,    1644, 1642, 1,    0,    0,    0,    1645, 1646,
      5,    3,    0,    0,    1646, 1862, 1,    0,    0,    0,    1647, 1648,
      5,    178,  0,    0,    1648, 1649, 5,    2,    0,    0,    1649, 1650,
      3,    94,   47,   0,    1650, 1651, 5,    19,   0,    0,    1651, 1659,
      3,    166,  83,   0,    1652, 1653, 5,    4,    0,    0,    1653, 1654,
      3,    94,   47,   0,    1654, 1655, 5,    19,   0,    0,    1655, 1656,
      3,    166,  83,   0,    1656, 1658, 1,    0,    0,    0,    1657, 1652,
      1,    0,    0,    0,    1658, 1661, 1,    0,    0,    0,    1659, 1657,
      1,    0,    0,    0,    1659, 1660, 1,    0,    0,    0,    1660, 1662,
      1,    0,    0,    0,    1661, 1659, 1,    0,    0,    0,    1662, 1663,
      5,    3,    0,    0,    1663, 1862, 1,    0,    0,    0,    1664, 1665,
      3,    154,  77,   0,    1665, 1666, 5,    2,    0,    0,    1666, 1667,
      5,    243,  0,    0,    1667, 1669, 5,    3,    0,    0,    1668, 1670,
      3,    132,  66,   0,    1669, 1668, 1,    0,    0,    0,    1669, 1670,
      1,    0,    0,    0,    1670, 1672, 1,    0,    0,    0,    1671, 1673,
      3,    134,  67,   0,    1672, 1671, 1,    0,    0,    0,    1672, 1673,
      1,    0,    0,    0,    1673, 1862, 1,    0,    0,    0,    1674, 1675,
      3,    154,  77,   0,    1675, 1687, 5,    2,    0,    0,    1676, 1678,
      3,    66,   33,   0,    1677, 1676, 1,    0,    0,    0,    1677, 1678,
      1,    0,    0,    0,    1678, 1679, 1,    0,    0,    0,    1679, 1684,
      3,    94,   47,   0,    1680, 1681, 5,    4,    0,    0,    1681, 1683,
      3,    94,   47,   0,    1682, 1680, 1,    0,    0,    0,    1683, 1686,
      1,    0,    0,    0,    1684, 1682, 1,    0,    0,    0,    1684, 1685,
      1,    0,    0,    0,    1685, 1688, 1,    0,    0,    0,    1686, 1684,
      1,    0,    0,    0,    1687, 1677, 1,    0,    0,    0,    1687, 1688,
      1,    0,    0,    0,    1688, 1699, 1,    0,    0,    0,    1689, 1690,
      5,    146,  0,    0,    1690, 1691, 5,    25,   0,    0,    1691, 1696,
      3,    52,   26,   0,    1692, 1693, 5,    4,    0,    0,    1693, 1695,
      3,    52,   26,   0,    1694, 1692, 1,    0,    0,    0,    1695, 1698,
      1,    0,    0,    0,    1696, 1694, 1,    0,    0,    0,    1696, 1697,
      1,    0,    0,    0,    1697, 1700, 1,    0,    0,    0,    1698, 1696,
      1,    0,    0,    0,    1699, 1689, 1,    0,    0,    0,    1699, 1700,
      1,    0,    0,    0,    1700, 1701, 1,    0,    0,    0,    1701, 1703,
      5,    3,    0,    0,    1702, 1704, 3,    132,  66,   0,    1703, 1702,
      1,    0,    0,    0,    1703, 1704, 1,    0,    0,    0,    1704, 1709,
      1,    0,    0,    0,    1705, 1707, 3,    106,  53,   0,    1706, 1705,
      1,    0,    0,    0,    1706, 1707, 1,    0,    0,    0,    1707, 1708,
      1,    0,    0,    0,    1708, 1710, 3,    134,  67,   0,    1709, 1706,
      1,    0,    0,    0,    1709, 1710, 1,    0,    0,    0,    1710, 1862,
      1,    0,    0,    0,    1711, 1712, 3,    166,  83,   0,    1712, 1713,
      5,    6,    0,    0,    1713, 1714, 3,    94,   47,   0,    1714, 1862,
      1,    0,    0,    0,    1715, 1724, 5,    2,    0,    0,    1716, 1721,
      3,    166,  83,   0,    1717, 1718, 5,    4,    0,    0,    1718, 1720,
      3,    166,  83,   0,    1719, 1717, 1,    0,    0,    0,    1720, 1723,
      1,    0,    0,    0,    1721, 1719, 1,    0,    0,    0,    1721, 1722,
      1,    0,    0,    0,    1722, 1725, 1,    0,    0,    0,    1723, 1721,
      1,    0,    0,    0,    1724, 1716, 1,    0,    0,    0,    1724, 1725,
      1,    0,    0,    0,    1725, 1726, 1,    0,    0,    0,    1726, 1727,
      5,    3,    0,    0,    1727, 1728, 5,    6,    0,    0,    1728, 1862,
      3,    94,   47,   0,    1729, 1730, 5,    2,    0,    0,    1730, 1731,
      3,    8,    4,    0,    1731, 1732, 5,    3,    0,    0,    1732, 1862,
      1,    0,    0,    0,    1733, 1734, 5,    70,   0,    0,    1734, 1735,
      5,    2,    0,    0,    1735, 1736, 3,    8,    4,    0,    1736, 1737,
      5,    3,    0,    0,    1737, 1862, 1,    0,    0,    0,    1738, 1739,
      5,    29,   0,    0,    1739, 1741, 3,    100,  50,   0,    1740, 1742,
      3,    130,  65,   0,    1741, 1740, 1,    0,    0,    0,    1742, 1743,
      1,    0,    0,    0,    1743, 1741, 1,    0,    0,    0,    1743, 1744,
      1,    0,    0,    0,    1744, 1747, 1,    0,    0,    0,    1745, 1746,
      5,    60,   0,    0,    1746, 1748, 3,    94,   47,   0,    1747, 1745,
      1,    0,    0,    0,    1747, 1748, 1,    0,    0,    0,    1748, 1749,
      1,    0,    0,    0,    1749, 1750, 5,    62,   0,    0,    1750, 1862,
      1,    0,    0,    0,    1751, 1753, 5,    29,   0,    0,    1752, 1754,
      3,    130,  65,   0,    1753, 1752, 1,    0,    0,    0,    1754, 1755,
      1,    0,    0,    0,    1755, 1753, 1,    0,    0,    0,    1755, 1756,
      1,    0,    0,    0,    1756, 1759, 1,    0,    0,    0,    1757, 1758,
      5,    60,   0,    0,    1758, 1760, 3,    94,   47,   0,    1759, 1757,
      1,    0,    0,    0,    1759, 1760, 1,    0,    0,    0,    1760, 1761,
      1,    0,    0,    0,    1761, 1762, 5,    62,   0,    0,    1762, 1862,
      1,    0,    0,    0,    1763, 1764, 5,    30,   0,    0,    1764, 1765,
      5,    2,    0,    0,    1765, 1766, 3,    94,   47,   0,    1766, 1767,
      5,    19,   0,    0,    1767, 1768, 3,    124,  62,   0,    1768, 1769,
      5,    3,    0,    0,    1769, 1862, 1,    0,    0,    0,    1770, 1771,
      5,    210,  0,    0,    1771, 1772, 5,    2,    0,    0,    1772, 1773,
      3,    94,   47,   0,    1773, 1774, 5,    19,   0,    0,    1774, 1775,
      3,    124,  62,   0,    1775, 1776, 5,    3,    0,    0,    1776, 1862,
      1,    0,    0,    0,    1777, 1778, 5,    18,   0,    0,    1778, 1787,
      5,    7,    0,    0,    1779, 1784, 3,    94,   47,   0,    1780, 1781,
      5,    4,    0,    0,    1781, 1783, 3,    94,   47,   0,    1782, 1780,
      1,    0,    0,    0,    1783, 1786, 1,    0,    0,    0,    1784, 1782,
      1,    0,    0,    0,    1784, 1785, 1,    0,    0,    0,    1785, 1788,
      1,    0,    0,    0,    1786, 1784, 1,    0,    0,    0,    1787, 1779,
      1,    0,    0,    0,    1787, 1788, 1,    0,    0,    0,    1788, 1789,
      1,    0,    0,    0,    1789, 1862, 5,    8,    0,    0,    1790, 1862,
      3,    166,  83,   0,    1791, 1862, 5,    42,   0,    0,    1792, 1796,
      5,    44,   0,    0,    1793, 1794, 5,    2,    0,    0,    1794, 1795,
      5,    250,  0,    0,    1795, 1797, 5,    3,    0,    0,    1796, 1793,
      1,    0,    0,    0,    1796, 1797, 1,    0,    0,    0,    1797, 1862,
      1,    0,    0,    0,    1798, 1802, 5,    45,   0,    0,    1799, 1800,
      5,    2,    0,    0,    1800, 1801, 5,    250,  0,    0,    1801, 1803,
      5,    3,    0,    0,    1802, 1799, 1,    0,    0,    0,    1802, 1803,
      1,    0,    0,    0,    1803, 1862, 1,    0,    0,    0,    1804, 1808,
      5,    119,  0,    0,    1805, 1806, 5,    2,    0,    0,    1806, 1807,
      5,    250,  0,    0,    1807, 1809, 5,    3,    0,    0,    1808, 1805,
      1,    0,    0,    0,    1808, 1809, 1,    0,    0,    0,    1809, 1862,
      1,    0,    0,    0,    1810, 1814, 5,    120,  0,    0,    1811, 1812,
      5,    2,    0,    0,    1812, 1813, 5,    250,  0,    0,    1813, 1815,
      5,    3,    0,    0,    1814, 1811, 1,    0,    0,    0,    1814, 1815,
      1,    0,    0,    0,    1815, 1862, 1,    0,    0,    0,    1816, 1862,
      5,    46,   0,    0,    1817, 1818, 5,    194,  0,    0,    1818, 1819,
      5,    2,    0,    0,    1819, 1820, 3,    100,  50,   0,    1820, 1821,
      5,    81,   0,    0,    1821, 1824, 3,    100,  50,   0,    1822, 1823,
      5,    79,   0,    0,    1823, 1825, 3,    100,  50,   0,    1824, 1822,
      1,    0,    0,    0,    1824, 1825, 1,    0,    0,    0,    1825, 1826,
      1,    0,    0,    0,    1826, 1827, 5,    3,    0,    0,    1827, 1862,
      1,    0,    0,    0,    1828, 1829, 5,    134,  0,    0,    1829, 1830,
      5,    2,    0,    0,    1830, 1833, 3,    100,  50,   0,    1831, 1832,
      5,    4,    0,    0,    1832, 1834, 3,    120,  60,   0,    1833, 1831,
      1,    0,    0,    0,    1833, 1834, 1,    0,    0,    0,    1834, 1835,
      1,    0,    0,    0,    1835, 1836, 5,    3,    0,    0,    1836, 1862,
      1,    0,    0,    0,    1837, 1838, 5,    72,   0,    0,    1838, 1839,
      5,    2,    0,    0,    1839, 1840, 3,    166,  83,   0,    1840, 1841,
      5,    81,   0,    0,    1841, 1842, 3,    100,  50,   0,    1842, 1843,
      5,    3,    0,    0,    1843, 1862, 1,    0,    0,    0,    1844, 1845,
      5,    2,    0,    0,    1845, 1846, 3,    94,   47,   0,    1846, 1847,
      5,    3,    0,    0,    1847, 1862, 1,    0,    0,    0,    1848, 1849,
      5,    91,   0,    0,    1849, 1858, 5,    2,    0,    0,    1850, 1855,
      3,    154,  77,   0,    1851, 1852, 5,    4,    0,    0,    1852, 1854,
      3,    154,  77,   0,    1853, 1851, 1,    0,    0,    0,    1854, 1857,
      1,    0,    0,    0,    1855, 1853, 1,    0,    0,    0,    1855, 1856,
      1,    0,    0,    0,    1856, 1859, 1,    0,    0,    0,    1857, 1855,
      1,    0,    0,    0,    1858, 1850, 1,    0,    0,    0,    1858, 1859,
      1,    0,    0,    0,    1859, 1860, 1,    0,    0,    0,    1860, 1862,
      5,    3,    0,    0,    1861, 1605, 1,    0,    0,    0,    1861, 1607,
      1,    0,    0,    0,    1861, 1608, 1,    0,    0,    0,    1861, 1611,
      1,    0,    0,    0,    1861, 1613, 1,    0,    0,    0,    1861, 1614,
      1,    0,    0,    0,    1861, 1615, 1,    0,    0,    0,    1861, 1616,
      1,    0,    0,    0,    1861, 1617, 1,    0,    0,    0,    1861, 1618,
      1,    0,    0,    0,    1861, 1625, 1,    0,    0,    0,    1861, 1635,
      1,    0,    0,    0,    1861, 1647, 1,    0,    0,    0,    1861, 1664,
      1,    0,    0,    0,    1861, 1674, 1,    0,    0,    0,    1861, 1711,
      1,    0,    0,    0,    1861, 1715, 1,    0,    0,    0,    1861, 1729,
      1,    0,    0,    0,    1861, 1733, 1,    0,    0,    0,    1861, 1738,
      1,    0,    0,    0,    1861, 1751, 1,    0,    0,    0,    1861, 1763,
      1,    0,    0,    0,    1861, 1770, 1,    0,    0,    0,    1861, 1777,
      1,    0,    0,    0,    1861, 1790, 1,    0,    0,    0,    1861, 1791,
      1,    0,    0,    0,    1861, 1792, 1,    0,    0,    0,    1861, 1798,
      1,    0,    0,    0,    1861, 1804, 1,    0,    0,    0,    1861, 1810,
      1,    0,    0,    0,    1861, 1816, 1,    0,    0,    0,    1861, 1817,
      1,    0,    0,    0,    1861, 1828, 1,    0,    0,    0,    1861, 1837,
      1,    0,    0,    0,    1861, 1844, 1,    0,    0,    0,    1861, 1848,
      1,    0,    0,    0,    1862, 1889, 1,    0,    0,    0,    1863, 1864,
      10,   15,   0,    0,    1864, 1865, 5,    7,    0,    0,    1865, 1866,
      3,    100,  50,   0,    1866, 1867, 5,    8,    0,    0,    1867, 1888,
      1,    0,    0,    0,    1868, 1869, 10,   13,   0,    0,    1869, 1870,
      5,    1,    0,    0,    1870, 1871, 3,    166,  83,   0,    1871, 1880,
      5,    2,    0,    0,    1872, 1877, 3,    94,   47,   0,    1873, 1874,
      5,    4,    0,    0,    1874, 1876, 3,    94,   47,   0,    1875, 1873,
      1,    0,    0,    0,    1876, 1879, 1,    0,    0,    0,    1877, 1875,
      1,    0,    0,    0,    1877, 1878, 1,    0,    0,    0,    1878, 1881,
      1,    0,    0,    0,    1879, 1877, 1,    0,    0,    0,    1880, 1872,
      1,    0,    0,    0,    1880, 1881, 1,    0,    0,    0,    1881, 1882,
      1,    0,    0,    0,    1882, 1883, 5,    3,    0,    0,    1883, 1888,
      1,    0,    0,    0,    1884, 1885, 10,   12,   0,    0,    1885, 1886,
      5,    1,    0,    0,    1886, 1888, 3,    166,  83,   0,    1887, 1863,
      1,    0,    0,    0,    1887, 1868, 1,    0,    0,    0,    1887, 1884,
      1,    0,    0,    0,    1888, 1891, 1,    0,    0,    0,    1889, 1887,
      1,    0,    0,    0,    1889, 1890, 1,    0,    0,    0,    1890, 103,
      1,    0,    0,    0,    1891, 1889, 1,    0,    0,    0,    1892, 1899,
      5,    247,  0,    0,    1893, 1896, 5,    248,  0,    0,    1894, 1895,
      5,    212,  0,    0,    1895, 1897, 5,    247,  0,    0,    1896, 1894,
      1,    0,    0,    0,    1896, 1897, 1,    0,    0,    0,    1897, 1899,
      1,    0,    0,    0,    1898, 1892, 1,    0,    0,    0,    1898, 1893,
      1,    0,    0,    0,    1899, 105,  1,    0,    0,    0,    1900, 1901,
      5,    96,   0,    0,    1901, 1905, 5,    138,  0,    0,    1902, 1903,
      5,    168,  0,    0,    1903, 1905, 5,    138,  0,    0,    1904, 1900,
      1,    0,    0,    0,    1904, 1902, 1,    0,    0,    0,    1905, 107,
      1,    0,    0,    0,    1906, 1907, 5,    204,  0,    0,    1907, 1908,
      5,    234,  0,    0,    1908, 1913, 3,    116,  58,   0,    1909, 1910,
      5,    204,  0,    0,    1910, 1911, 5,    234,  0,    0,    1911, 1913,
      3,    104,  52,   0,    1912, 1906, 1,    0,    0,    0,    1912, 1909,
      1,    0,    0,    0,    1913, 109,  1,    0,    0,    0,    1914, 1915,
      7,    13,   0,    0,    1915, 111,  1,    0,    0,    0,    1916, 1917,
      7,    14,   0,    0,    1917, 113,  1,    0,    0,    0,    1918, 1919,
      7,    15,   0,    0,    1919, 115,  1,    0,    0,    0,    1920, 1922,
      5,    103,  0,    0,    1921, 1923, 7,    11,   0,    0,    1922, 1921,
      1,    0,    0,    0,    1922, 1923, 1,    0,    0,    0,    1923, 1924,
      1,    0,    0,    0,    1924, 1925, 3,    104,  52,   0,    1925, 1928,
      3,    118,  59,   0,    1926, 1927, 5,    206,  0,    0,    1927, 1929,
      3,    118,  59,   0,    1928, 1926, 1,    0,    0,    0,    1928, 1929,
      1,    0,    0,    0,    1929, 117,  1,    0,    0,    0,    1930, 1931,
      7,    16,   0,    0,    1931, 119,  1,    0,    0,    0,    1932, 1933,
      7,    17,   0,    0,    1933, 121,  1,    0,    0,    0,    1934, 1943,
      5,    2,    0,    0,    1935, 1940, 3,    124,  62,   0,    1936, 1937,
      5,    4,    0,    0,    1937, 1939, 3,    124,  62,   0,    1938, 1936,
      1,    0,    0,    0,    1939, 1942, 1,    0,    0,    0,    1940, 1938,
      1,    0,    0,    0,    1940, 1941, 1,    0,    0,    0,    1941, 1944,
      1,    0,    0,    0,    1942, 1940, 1,    0,    0,    0,    1943, 1935,
      1,    0,    0,    0,    1943, 1944, 1,    0,    0,    0,    1944, 1945,
      1,    0,    0,    0,    1945, 1946, 5,    3,    0,    0,    1946, 123,
      1,    0,    0,    0,    1947, 1948, 6,    62,   -1,   0,    1948, 1949,
      5,    18,   0,    0,    1949, 1950, 5,    237,  0,    0,    1950, 1951,
      3,    124,  62,   0,    1951, 1952, 5,    239,  0,    0,    1952, 1995,
      1,    0,    0,    0,    1953, 1954, 5,    122,  0,    0,    1954, 1955,
      5,    237,  0,    0,    1955, 1956, 3,    124,  62,   0,    1956, 1957,
      5,    4,    0,    0,    1957, 1958, 3,    124,  62,   0,    1958, 1959,
      5,    239,  0,    0,    1959, 1995, 1,    0,    0,    0,    1960, 1961,
      5,    178,  0,    0,    1961, 1962, 5,    2,    0,    0,    1962, 1963,
      3,    166,  83,   0,    1963, 1970, 3,    124,  62,   0,    1964, 1965,
      5,    4,    0,    0,    1965, 1966, 3,    166,  83,   0,    1966, 1967,
      3,    124,  62,   0,    1967, 1969, 1,    0,    0,    0,    1968, 1964,
      1,    0,    0,    0,    1969, 1972, 1,    0,    0,    0,    1970, 1968,
      1,    0,    0,    0,    1970, 1971, 1,    0,    0,    0,    1971, 1973,
      1,    0,    0,    0,    1972, 1970, 1,    0,    0,    0,    1973, 1974,
      5,    3,    0,    0,    1974, 1995, 1,    0,    0,    0,    1975, 1987,
      3,    128,  64,   0,    1976, 1977, 5,    2,    0,    0,    1977, 1982,
      3,    126,  63,   0,    1978, 1979, 5,    4,    0,    0,    1979, 1981,
      3,    126,  63,   0,    1980, 1978, 1,    0,    0,    0,    1981, 1984,
      1,    0,    0,    0,    1982, 1980, 1,    0,    0,    0,    1982, 1983,
      1,    0,    0,    0,    1983, 1985, 1,    0,    0,    0,    1984, 1982,
      1,    0,    0,    0,    1985, 1986, 5,    3,    0,    0,    1986, 1988,
      1,    0,    0,    0,    1987, 1976, 1,    0,    0,    0,    1987, 1988,
      1,    0,    0,    0,    1988, 1995, 1,    0,    0,    0,    1989, 1990,
      5,    103,  0,    0,    1990, 1991, 3,    118,  59,   0,    1991, 1992,
      5,    206,  0,    0,    1992, 1993, 3,    118,  59,   0,    1993, 1995,
      1,    0,    0,    0,    1994, 1947, 1,    0,    0,    0,    1994, 1953,
      1,    0,    0,    0,    1994, 1960, 1,    0,    0,    0,    1994, 1975,
      1,    0,    0,    0,    1994, 1989, 1,    0,    0,    0,    1995, 2000,
      1,    0,    0,    0,    1996, 1997, 10,   6,    0,    0,    1997, 1999,
      5,    18,   0,    0,    1998, 1996, 1,    0,    0,    0,    1999, 2002,
      1,    0,    0,    0,    2000, 1998, 1,    0,    0,    0,    2000, 2001,
      1,    0,    0,    0,    2001, 125,  1,    0,    0,    0,    2002, 2000,
      1,    0,    0,    0,    2003, 2006, 5,    250,  0,    0,    2004, 2006,
      3,    124,  62,   0,    2005, 2003, 1,    0,    0,    0,    2005, 2004,
      1,    0,    0,    0,    2006, 127,  1,    0,    0,    0,    2007, 2012,
      5,    257,  0,    0,    2008, 2012, 5,    258,  0,    0,    2009, 2012,
      5,    259,  0,    0,    2010, 2012, 3,    154,  77,   0,    2011, 2007,
      1,    0,    0,    0,    2011, 2008, 1,    0,    0,    0,    2011, 2009,
      1,    0,    0,    0,    2011, 2010, 1,    0,    0,    0,    2012, 129,
      1,    0,    0,    0,    2013, 2014, 5,    227,  0,    0,    2014, 2015,
      3,    94,   47,   0,    2015, 2016, 5,    203,  0,    0,    2016, 2017,
      3,    94,   47,   0,    2017, 131,  1,    0,    0,    0,    2018, 2019,
      5,    76,   0,    0,    2019, 2020, 5,    2,    0,    0,    2020, 2021,
      5,    228,  0,    0,    2021, 2022, 3,    96,   48,   0,    2022, 2023,
      5,    3,    0,    0,    2023, 133,  1,    0,    0,    0,    2024, 2025,
      5,    150,  0,    0,    2025, 2032, 3,    166,  83,   0,    2026, 2027,
      5,    150,  0,    0,    2027, 2028, 5,    2,    0,    0,    2028, 2029,
      3,    136,  68,   0,    2029, 2030, 5,    3,    0,    0,    2030, 2032,
      1,    0,    0,    0,    2031, 2024, 1,    0,    0,    0,    2031, 2026,
      1,    0,    0,    0,    2032, 135,  1,    0,    0,    0,    2033, 2035,
      3,    166,  83,   0,    2034, 2033, 1,    0,    0,    0,    2034, 2035,
      1,    0,    0,    0,    2035, 2046, 1,    0,    0,    0,    2036, 2037,
      5,    151,  0,    0,    2037, 2038, 5,    25,   0,    0,    2038, 2043,
      3,    94,   47,   0,    2039, 2040, 5,    4,    0,    0,    2040, 2042,
      3,    94,   47,   0,    2041, 2039, 1,    0,    0,    0,    2042, 2045,
      1,    0,    0,    0,    2043, 2041, 1,    0,    0,    0,    2043, 2044,
      1,    0,    0,    0,    2044, 2047, 1,    0,    0,    0,    2045, 2043,
      1,    0,    0,    0,    2046, 2036, 1,    0,    0,    0,    2046, 2047,
      1,    0,    0,    0,    2047, 2058, 1,    0,    0,    0,    2048, 2049,
      5,    146,  0,    0,    2049, 2050, 5,    25,   0,    0,    2050, 2055,
      3,    52,   26,   0,    2051, 2052, 5,    4,    0,    0,    2052, 2054,
      3,    52,   26,   0,    2053, 2051, 1,    0,    0,    0,    2054, 2057,
      1,    0,    0,    0,    2055, 2053, 1,    0,    0,    0,    2055, 2056,
      1,    0,    0,    0,    2056, 2059, 1,    0,    0,    0,    2057, 2055,
      1,    0,    0,    0,    2058, 2048, 1,    0,    0,    0,    2058, 2059,
      1,    0,    0,    0,    2059, 2061, 1,    0,    0,    0,    2060, 2062,
      3,    138,  69,   0,    2061, 2060, 1,    0,    0,    0,    2061, 2062,
      1,    0,    0,    0,    2062, 137,  1,    0,    0,    0,    2063, 2064,
      5,    159,  0,    0,    2064, 2088, 3,    140,  70,   0,    2065, 2066,
      5,    179,  0,    0,    2066, 2088, 3,    140,  70,   0,    2067, 2068,
      5,    92,   0,    0,    2068, 2088, 3,    140,  70,   0,    2069, 2070,
      5,    159,  0,    0,    2070, 2071, 5,    24,   0,    0,    2071, 2072,
      3,    140,  70,   0,    2072, 2073, 5,    16,   0,    0,    2073, 2074,
      3,    140,  70,   0,    2074, 2088, 1,    0,    0,    0,    2075, 2076,
      5,    179,  0,    0,    2076, 2077, 5,    24,   0,    0,    2077, 2078,
      3,    140,  70,   0,    2078, 2079, 5,    16,   0,    0,    2079, 2080,
      3,    140,  70,   0,    2080, 2088, 1,    0,    0,    0,    2081, 2082,
      5,    92,   0,    0,    2082, 2083, 5,    24,   0,    0,    2083, 2084,
      3,    140,  70,   0,    2084, 2085, 5,    16,   0,    0,    2085, 2086,
      3,    140,  70,   0,    2086, 2088, 1,    0,    0,    0,    2087, 2063,
      1,    0,    0,    0,    2087, 2065, 1,    0,    0,    0,    2087, 2067,
      1,    0,    0,    0,    2087, 2069, 1,    0,    0,    0,    2087, 2075,
      1,    0,    0,    0,    2087, 2081, 1,    0,    0,    0,    2088, 139,
      1,    0,    0,    0,    2089, 2090, 5,    213,  0,    0,    2090, 2099,
      5,    154,  0,    0,    2091, 2092, 5,    213,  0,    0,    2092, 2099,
      5,    78,   0,    0,    2093, 2094, 5,    41,   0,    0,    2094, 2099,
      5,    178,  0,    0,    2095, 2096, 3,    94,   47,   0,    2096, 2097,
      7,    18,   0,    0,    2097, 2099, 1,    0,    0,    0,    2098, 2089,
      1,    0,    0,    0,    2098, 2091, 1,    0,    0,    0,    2098, 2093,
      1,    0,    0,    0,    2098, 2095, 1,    0,    0,    0,    2099, 141,
      1,    0,    0,    0,    2100, 2101, 3,    166,  83,   0,    2101, 2102,
      5,    235,  0,    0,    2102, 2103, 3,    94,   47,   0,    2103, 143,
      1,    0,    0,    0,    2104, 2105, 5,    80,   0,    0,    2105, 2113,
      7,    19,   0,    0,    2106, 2107, 5,    211,  0,    0,    2107, 2110,
      7,    20,   0,    0,    2108, 2109, 5,    229,  0,    0,    2109, 2111,
      3,    18,   9,    0,    2110, 2108, 1,    0,    0,    0,    2110, 2111,
      1,    0,    0,    0,    2111, 2113, 1,    0,    0,    0,    2112, 2104,
      1,    0,    0,    0,    2112, 2106, 1,    0,    0,    0,    2113, 145,
      1,    0,    0,    0,    2114, 2115, 5,    108,  0,    0,    2115, 2116,
      5,    116,  0,    0,    2116, 2120, 3,    148,  74,   0,    2117, 2118,
      5,    160,  0,    0,    2118, 2120, 7,    21,   0,    0,    2119, 2114,
      1,    0,    0,    0,    2119, 2117, 1,    0,    0,    0,    2120, 147,
      1,    0,    0,    0,    2121, 2122, 5,    160,  0,    0,    2122, 2129,
      5,    214,  0,    0,    2123, 2124, 5,    160,  0,    0,    2124, 2129,
      5,    36,   0,    0,    2125, 2126, 5,    165,  0,    0,    2126, 2129,
      5,    160,  0,    0,    2127, 2129, 5,    185,  0,    0,    2128, 2121,
      1,    0,    0,    0,    2128, 2123, 1,    0,    0,    0,    2128, 2125,
      1,    0,    0,    0,    2128, 2127, 1,    0,    0,    0,    2129, 149,
      1,    0,    0,    0,    2130, 2136, 3,    94,   47,   0,    2131, 2132,
      3,    166,  83,   0,    2132, 2133, 5,    9,    0,    0,    2133, 2134,
      3,    94,   47,   0,    2134, 2136, 1,    0,    0,    0,    2135, 2130,
      1,    0,    0,    0,    2135, 2131, 1,    0,    0,    0,    2136, 151,
      1,    0,    0,    0,    2137, 2142, 5,    184,  0,    0,    2138, 2142,
      5,    52,   0,    0,    2139, 2142, 5,    101,  0,    0,    2140, 2142,
      3,    166,  83,   0,    2141, 2137, 1,    0,    0,    0,    2141, 2138,
      1,    0,    0,    0,    2141, 2139, 1,    0,    0,    0,    2141, 2140,
      1,    0,    0,    0,    2142, 153,  1,    0,    0,    0,    2143, 2148,
      3,    166,  83,   0,    2144, 2145, 5,    1,    0,    0,    2145, 2147,
      3,    166,  83,   0,    2146, 2144, 1,    0,    0,    0,    2147, 2150,
      1,    0,    0,    0,    2148, 2146, 1,    0,    0,    0,    2148, 2149,
      1,    0,    0,    0,    2149, 155,  1,    0,    0,    0,    2150, 2148,
      1,    0,    0,    0,    2151, 2152, 5,    79,   0,    0,    2152, 2153,
      7,    22,   0,    0,    2153, 2154, 3,    158,  79,   0,    2154, 2155,
      3,    100,  50,   0,    2155, 157,  1,    0,    0,    0,    2156, 2157,
      5,    19,   0,    0,    2157, 2160, 5,    139,  0,    0,    2158, 2160,
      5,    22,   0,    0,    2159, 2156, 1,    0,    0,    0,    2159, 2158,
      1,    0,    0,    0,    2160, 159,  1,    0,    0,    0,    2161, 2165,
      5,    46,   0,    0,    2162, 2165, 5,    43,   0,    0,    2163, 2165,
      3,    162,  81,   0,    2164, 2161, 1,    0,    0,    0,    2164, 2162,
      1,    0,    0,    0,    2164, 2163, 1,    0,    0,    0,    2165, 161,
      1,    0,    0,    0,    2166, 2167, 5,    220,  0,    0,    2167, 2172,
      3,    166,  83,   0,    2168, 2169, 5,    174,  0,    0,    2169, 2172,
      3,    166,  83,   0,    2170, 2172, 3,    166,  83,   0,    2171, 2166,
      1,    0,    0,    0,    2171, 2168, 1,    0,    0,    0,    2171, 2170,
      1,    0,    0,    0,    2172, 163,  1,    0,    0,    0,    2173, 2178,
      3,    166,  83,   0,    2174, 2175, 5,    4,    0,    0,    2175, 2177,
      3,    166,  83,   0,    2176, 2174, 1,    0,    0,    0,    2177, 2180,
      1,    0,    0,    0,    2178, 2176, 1,    0,    0,    0,    2178, 2179,
      1,    0,    0,    0,    2179, 165,  1,    0,    0,    0,    2180, 2178,
      1,    0,    0,    0,    2181, 2187, 5,    253,  0,    0,    2182, 2187,
      5,    255,  0,    0,    2183, 2187, 3,    188,  94,   0,    2184, 2187,
      5,    256,  0,    0,    2185, 2187, 5,    254,  0,    0,    2186, 2181,
      1,    0,    0,    0,    2186, 2182, 1,    0,    0,    0,    2186, 2183,
      1,    0,    0,    0,    2186, 2184, 1,    0,    0,    0,    2186, 2185,
      1,    0,    0,    0,    2187, 167,  1,    0,    0,    0,    2188, 2192,
      5,    251,  0,    0,    2189, 2192, 5,    252,  0,    0,    2190, 2192,
      5,    250,  0,    0,    2191, 2188, 1,    0,    0,    0,    2191, 2189,
      1,    0,    0,    0,    2191, 2190, 1,    0,    0,    0,    2192, 169,
      1,    0,    0,    0,    2193, 2196, 3,    172,  86,   0,    2194, 2196,
      3,    174,  87,   0,    2195, 2193, 1,    0,    0,    0,    2195, 2194,
      1,    0,    0,    0,    2196, 171,  1,    0,    0,    0,    2197, 2198,
      5,    37,   0,    0,    2198, 2199, 3,    166,  83,   0,    2199, 2200,
      3,    174,  87,   0,    2200, 173,  1,    0,    0,    0,    2201, 2202,
      3,    176,  88,   0,    2202, 2204, 3,    90,   45,   0,    2203, 2205,
      3,    178,  89,   0,    2204, 2203, 1,    0,    0,    0,    2204, 2205,
      1,    0,    0,    0,    2205, 175,  1,    0,    0,    0,    2206, 2210,
      5,    216,  0,    0,    2207, 2208, 5,    156,  0,    0,    2208, 2210,
      5,    111,  0,    0,    2209, 2206, 1,    0,    0,    0,    2209, 2207,
      1,    0,    0,    0,    2210, 177,  1,    0,    0,    0,    2211, 2213,
      3,    180,  90,   0,    2212, 2211, 1,    0,    0,    0,    2213, 2216,
      1,    0,    0,    0,    2214, 2212, 1,    0,    0,    0,    2214, 2215,
      1,    0,    0,    0,    2215, 179,  1,    0,    0,    0,    2216, 2214,
      1,    0,    0,    0,    2217, 2221, 3,    184,  92,   0,    2218, 2221,
      3,    182,  91,   0,    2219, 2221, 3,    186,  93,   0,    2220, 2217,
      1,    0,    0,    0,    2220, 2218, 1,    0,    0,    0,    2220, 2219,
      1,    0,    0,    0,    2221, 181,  1,    0,    0,    0,    2222, 2226,
      5,    163,  0,    0,    2223, 2224, 5,    135,  0,    0,    2224, 2226,
      5,    163,  0,    0,    2225, 2222, 1,    0,    0,    0,    2225, 2223,
      1,    0,    0,    0,    2226, 183,  1,    0,    0,    0,    2227, 2228,
      7,    23,   0,    0,    2228, 185,  1,    0,    0,    0,    2229, 2233,
      5,    63,   0,    0,    2230, 2231, 5,    135,  0,    0,    2231, 2233,
      5,    63,   0,    0,    2232, 2229, 1,    0,    0,    0,    2232, 2230,
      1,    0,    0,    0,    2233, 187,  1,    0,    0,    0,    2234, 2235,
      7,    24,   0,    0,    2235, 189,  1,    0,    0,    0,    288,  212,
      217,  223,  227,  241,  245,  249,  253,  261,  265,  268,  275,  284,
      290,  294,  300,  307,  316,  325,  336,  343,  353,  360,  368,  376,
      384,  394,  401,  409,  414,  425,  430,  441,  452,  464,  470,  475,
      481,  490,  501,  510,  515,  519,  527,  534,  547,  550,  560,  563,
      570,  579,  585,  590,  594,  604,  607,  617,  630,  636,  641,  647,
      656,  662,  669,  677,  682,  686,  694,  700,  707,  712,  716,  726,
      729,  733,  736,  744,  749,  770,  776,  782,  784,  790,  796,  798,
      806,  808,  827,  832,  839,  851,  853,  861,  863,  881,  884,  888,
      892,  910,  913,  929,  934,  936,  939,  945,  952,  956,  961,  967,
      971,  975,  981,  989,  1004, 1011, 1016, 1023, 1031, 1035, 1040, 1051,
      1063, 1066, 1071, 1073, 1082, 1084, 1092, 1098, 1101, 1103, 1115, 1119,
      1125, 1129, 1133, 1137, 1144, 1148, 1156, 1159, 1163, 1168, 1172, 1180,
      1183, 1191, 1196, 1201, 1205, 1213, 1216, 1218, 1227, 1234, 1245, 1248,
      1258, 1261, 1272, 1277, 1285, 1288, 1292, 1296, 1309, 1313, 1322, 1329,
      1333, 1336, 1338, 1342, 1344, 1353, 1365, 1391, 1395, 1399, 1403, 1407,
      1411, 1413, 1424, 1429, 1438, 1444, 1448, 1450, 1458, 1465, 1478, 1484,
      1495, 1502, 1506, 1514, 1516, 1529, 1537, 1546, 1552, 1560, 1566, 1570,
      1575, 1580, 1586, 1600, 1602, 1631, 1642, 1659, 1669, 1672, 1677, 1684,
      1687, 1696, 1699, 1703, 1706, 1709, 1721, 1724, 1743, 1747, 1755, 1759,
      1784, 1787, 1796, 1802, 1808, 1814, 1824, 1833, 1855, 1858, 1861, 1877,
      1880, 1887, 1889, 1896, 1898, 1904, 1912, 1922, 1928, 1940, 1943, 1970,
      1982, 1987, 1994, 2000, 2005, 2011, 2031, 2034, 2043, 2046, 2055, 2058,
      2061, 2087, 2098, 2110, 2112, 2119, 2128, 2135, 2141, 2148, 2159, 2164,
      2171, 2178, 2186, 2191, 2195, 2204, 2209, 2214, 2220, 2225, 2232};
  staticData->serializedATN = antlr4::atn::SerializedATNView(
      serializedATNSegment,
      sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    staticData->decisionToDFA.emplace_back(
        staticData->atn->getDecisionState(i), i);
  }
  prestosqlParserStaticData = std::move(staticData);
}

} // namespace

PrestoSqlParser::PrestoSqlParser(TokenStream* input)
    : PrestoSqlParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

PrestoSqlParser::PrestoSqlParser(
    TokenStream* input,
    const antlr4::atn::ParserATNSimulatorOptions& options)
    : Parser(input) {
  PrestoSqlParser::initialize();
  _interpreter = new atn::ParserATNSimulator(
      this,
      *prestosqlParserStaticData->atn,
      prestosqlParserStaticData->decisionToDFA,
      prestosqlParserStaticData->sharedContextCache,
      options);
}

PrestoSqlParser::~PrestoSqlParser() {
  delete _interpreter;
}

const atn::ATN& PrestoSqlParser::getATN() const {
  return *prestosqlParserStaticData->atn;
}

std::string PrestoSqlParser::getGrammarFileName() const {
  return "PrestoSql.g4";
}

const std::vector<std::string>& PrestoSqlParser::getRuleNames() const {
  return prestosqlParserStaticData->ruleNames;
}

const dfa::Vocabulary& PrestoSqlParser::getVocabulary() const {
  return prestosqlParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView PrestoSqlParser::getSerializedATN() const {
  return prestosqlParserStaticData->serializedATN;
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

std::any PrestoSqlParser::SingleStatementContext::accept(
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
    setState(190);
    statement();
    setState(191);
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

std::any PrestoSqlParser::StandaloneExpressionContext::accept(
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
    setState(193);
    expression();
    setState(194);
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

std::any PrestoSqlParser::StandaloneRoutineBodyContext::accept(
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
    setState(196);
    routineBody();
    setState(197);
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

std::any PrestoSqlParser::ExplainContext::accept(
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

std::any PrestoSqlParser::PrepareContext::accept(
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

std::any PrestoSqlParser::DropMaterializedViewContext::accept(
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

std::any PrestoSqlParser::UseContext::accept(tree::ParseTreeVisitor* visitor) {
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

std::any PrestoSqlParser::AddConstraintContext::accept(
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

std::any PrestoSqlParser::DeallocateContext::accept(
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

std::any PrestoSqlParser::RenameTableContext::accept(
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

std::any PrestoSqlParser::CommitContext::accept(
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

std::any PrestoSqlParser::CreateRoleContext::accept(
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

std::any PrestoSqlParser::ShowCreateFunctionContext::accept(
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

std::any PrestoSqlParser::DropColumnContext::accept(
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

std::any PrestoSqlParser::DropViewContext::accept(
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

std::any PrestoSqlParser::ShowTablesContext::accept(
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

std::any PrestoSqlParser::ShowCatalogsContext::accept(
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

std::any PrestoSqlParser::ShowRolesContext::accept(
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

std::any PrestoSqlParser::RenameColumnContext::accept(
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

std::any PrestoSqlParser::RevokeRolesContext::accept(
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

std::any PrestoSqlParser::ShowCreateTableContext::accept(
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

std::any PrestoSqlParser::ShowColumnsContext::accept(
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

std::any PrestoSqlParser::ShowRoleGrantsContext::accept(
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

std::any PrestoSqlParser::AddColumnContext::accept(
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

std::any PrestoSqlParser::ResetSessionContext::accept(
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

std::any PrestoSqlParser::DropConstraintContext::accept(
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

std::any PrestoSqlParser::InsertIntoContext::accept(
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

std::any PrestoSqlParser::ShowSessionContext::accept(
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

std::any PrestoSqlParser::CreateSchemaContext::accept(
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

std::any PrestoSqlParser::ExecuteContext::accept(
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

std::any PrestoSqlParser::RenameSchemaContext::accept(
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

std::any PrestoSqlParser::DropRoleContext::accept(
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

std::any PrestoSqlParser::AnalyzeContext::accept(
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

std::any PrestoSqlParser::SetRoleContext::accept(
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

std::any PrestoSqlParser::CreateFunctionContext::accept(
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

std::any PrestoSqlParser::ShowGrantsContext::accept(
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

std::any PrestoSqlParser::DropSchemaContext::accept(
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

std::any PrestoSqlParser::ShowCreateViewContext::accept(
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

std::any PrestoSqlParser::CreateTableContext::accept(
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

std::any PrestoSqlParser::StartTransactionContext::accept(
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

std::any PrestoSqlParser::CreateTableAsSelectContext::accept(
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

std::any PrestoSqlParser::ShowStatsContext::accept(
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

std::any PrestoSqlParser::DropFunctionContext::accept(
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

std::any PrestoSqlParser::RevokeContext::accept(
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

std::any PrestoSqlParser::UpdateContext::accept(
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

std::any PrestoSqlParser::CreateTypeContext::accept(
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

std::any PrestoSqlParser::DeleteContext::accept(
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

std::any PrestoSqlParser::DescribeInputContext::accept(
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

std::any PrestoSqlParser::ShowStatsForQueryContext::accept(
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

std::any PrestoSqlParser::StatementDefaultContext::accept(
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

std::any PrestoSqlParser::TruncateTableContext::accept(
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

std::any PrestoSqlParser::AlterColumnSetNotNullContext::accept(
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

std::any PrestoSqlParser::CreateMaterializedViewContext::accept(
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

std::any PrestoSqlParser::AlterFunctionContext::accept(
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

std::any PrestoSqlParser::SetSessionContext::accept(
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

std::any PrestoSqlParser::CreateViewContext::accept(
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

std::any PrestoSqlParser::ShowSchemasContext::accept(
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

std::any PrestoSqlParser::DropTableContext::accept(
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

std::any PrestoSqlParser::RollbackContext::accept(
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

std::any PrestoSqlParser::RenameViewContext::accept(
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

std::any PrestoSqlParser::AlterColumnDropNotNullContext::accept(
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

std::any PrestoSqlParser::GrantRolesContext::accept(
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

std::any PrestoSqlParser::CallContext::accept(tree::ParseTreeVisitor* visitor) {
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

std::any PrestoSqlParser::RefreshMaterializedViewContext::accept(
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

std::any PrestoSqlParser::ShowCreateMaterializedViewContext::accept(
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

std::any PrestoSqlParser::ShowFunctionsContext::accept(
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

std::any PrestoSqlParser::DescribeOutputContext::accept(
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

std::any PrestoSqlParser::GrantContext::accept(
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

std::any PrestoSqlParser::SetTablePropertiesContext::accept(
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
    setState(936);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 102, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::StatementDefaultContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(199);
        query();
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UseContext>(_localctx);
        enterOuterAlt(_localctx, 2);
        setState(200);
        match(PrestoSqlParser::USE);
        setState(201);
        antlrcpp::downCast<UseContext*>(_localctx)->schema = identifier();
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UseContext>(_localctx);
        enterOuterAlt(_localctx, 3);
        setState(202);
        match(PrestoSqlParser::USE);
        setState(203);
        antlrcpp::downCast<UseContext*>(_localctx)->catalog = identifier();
        setState(204);
        match(PrestoSqlParser::T__0);
        setState(205);
        antlrcpp::downCast<UseContext*>(_localctx)->schema = identifier();
        break;
      }

      case 4: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CreateSchemaContext>(
                _localctx);
        enterOuterAlt(_localctx, 4);
        setState(207);
        match(PrestoSqlParser::CREATE);
        setState(208);
        match(PrestoSqlParser::SCHEMA);
        setState(212);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 0, _ctx)) {
          case 1: {
            setState(209);
            match(PrestoSqlParser::IF);
            setState(210);
            match(PrestoSqlParser::NOT);
            setState(211);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(214);
        qualifiedName();
        setState(217);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(215);
          match(PrestoSqlParser::WITH);
          setState(216);
          properties();
        }
        break;
      }

      case 5: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DropSchemaContext>(
            _localctx);
        enterOuterAlt(_localctx, 5);
        setState(219);
        match(PrestoSqlParser::DROP);
        setState(220);
        match(PrestoSqlParser::SCHEMA);
        setState(223);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 2, _ctx)) {
          case 1: {
            setState(221);
            match(PrestoSqlParser::IF);
            setState(222);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(225);
        qualifiedName();
        setState(227);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::CASCADE ||
            _la == PrestoSqlParser::RESTRICT) {
          setState(226);
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
        setState(229);
        match(PrestoSqlParser::ALTER);
        setState(230);
        match(PrestoSqlParser::SCHEMA);
        setState(231);
        qualifiedName();
        setState(232);
        match(PrestoSqlParser::RENAME);
        setState(233);
        match(PrestoSqlParser::TO);
        setState(234);
        identifier();
        break;
      }

      case 7: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::CreateTableAsSelectContext>(
                    _localctx);
        enterOuterAlt(_localctx, 7);
        setState(236);
        match(PrestoSqlParser::CREATE);
        setState(237);
        match(PrestoSqlParser::TABLE);
        setState(241);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 4, _ctx)) {
          case 1: {
            setState(238);
            match(PrestoSqlParser::IF);
            setState(239);
            match(PrestoSqlParser::NOT);
            setState(240);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(243);
        qualifiedName();
        setState(245);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::T__1) {
          setState(244);
          columnAliases();
        }
        setState(249);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::COMMENT) {
          setState(247);
          match(PrestoSqlParser::COMMENT);
          setState(248);
          string();
        }
        setState(253);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(251);
          match(PrestoSqlParser::WITH);
          setState(252);
          properties();
        }
        setState(255);
        match(PrestoSqlParser::AS);
        setState(261);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 8, _ctx)) {
          case 1: {
            setState(256);
            query();
            break;
          }

          case 2: {
            setState(257);
            match(PrestoSqlParser::T__1);
            setState(258);
            query();
            setState(259);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        setState(268);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(263);
          match(PrestoSqlParser::WITH);
          setState(265);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::NO) {
            setState(264);
            match(PrestoSqlParser::NO);
          }
          setState(267);
          match(PrestoSqlParser::DATA);
        }
        break;
      }

      case 8: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CreateTableContext>(
                _localctx);
        enterOuterAlt(_localctx, 8);
        setState(270);
        match(PrestoSqlParser::CREATE);
        setState(271);
        match(PrestoSqlParser::TABLE);
        setState(275);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 11, _ctx)) {
          case 1: {
            setState(272);
            match(PrestoSqlParser::IF);
            setState(273);
            match(PrestoSqlParser::NOT);
            setState(274);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(277);
        qualifiedName();
        setState(278);
        match(PrestoSqlParser::T__1);
        setState(279);
        tableElement();
        setState(284);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(280);
          match(PrestoSqlParser::T__3);
          setState(281);
          tableElement();
          setState(286);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(287);
        match(PrestoSqlParser::T__2);
        setState(290);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::COMMENT) {
          setState(288);
          match(PrestoSqlParser::COMMENT);
          setState(289);
          string();
        }
        setState(294);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(292);
          match(PrestoSqlParser::WITH);
          setState(293);
          properties();
        }
        break;
      }

      case 9: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DropTableContext>(
            _localctx);
        enterOuterAlt(_localctx, 9);
        setState(296);
        match(PrestoSqlParser::DROP);
        setState(297);
        match(PrestoSqlParser::TABLE);
        setState(300);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 15, _ctx)) {
          case 1: {
            setState(298);
            match(PrestoSqlParser::IF);
            setState(299);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(302);
        qualifiedName();
        break;
      }

      case 10: {
        _localctx = _tracker.createInstance<PrestoSqlParser::InsertIntoContext>(
            _localctx);
        enterOuterAlt(_localctx, 10);
        setState(303);
        match(PrestoSqlParser::INSERT);
        setState(304);
        match(PrestoSqlParser::INTO);
        setState(305);
        qualifiedName();
        setState(307);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 16, _ctx)) {
          case 1: {
            setState(306);
            columnAliases();
            break;
          }

          default:
            break;
        }
        setState(309);
        query();
        break;
      }

      case 11: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DeleteContext>(_localctx);
        enterOuterAlt(_localctx, 11);
        setState(311);
        match(PrestoSqlParser::DELETE);
        setState(312);
        match(PrestoSqlParser::FROM);
        setState(313);
        qualifiedName();
        setState(316);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WHERE) {
          setState(314);
          match(PrestoSqlParser::WHERE);
          setState(315);
          booleanExpression(0);
        }
        break;
      }

      case 12: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TruncateTableContext>(
                _localctx);
        enterOuterAlt(_localctx, 12);
        setState(318);
        match(PrestoSqlParser::TRUNCATE);
        setState(319);
        match(PrestoSqlParser::TABLE);
        setState(320);
        qualifiedName();
        break;
      }

      case 13: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RenameTableContext>(
                _localctx);
        enterOuterAlt(_localctx, 13);
        setState(321);
        match(PrestoSqlParser::ALTER);
        setState(322);
        match(PrestoSqlParser::TABLE);
        setState(325);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 18, _ctx)) {
          case 1: {
            setState(323);
            match(PrestoSqlParser::IF);
            setState(324);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(327);
        antlrcpp::downCast<RenameTableContext*>(_localctx)->from =
            qualifiedName();
        setState(328);
        match(PrestoSqlParser::RENAME);
        setState(329);
        match(PrestoSqlParser::TO);
        setState(330);
        antlrcpp::downCast<RenameTableContext*>(_localctx)->to =
            qualifiedName();
        break;
      }

      case 14: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RenameColumnContext>(
                _localctx);
        enterOuterAlt(_localctx, 14);
        setState(332);
        match(PrestoSqlParser::ALTER);
        setState(333);
        match(PrestoSqlParser::TABLE);
        setState(336);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 19, _ctx)) {
          case 1: {
            setState(334);
            match(PrestoSqlParser::IF);
            setState(335);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(338);
        antlrcpp::downCast<RenameColumnContext*>(_localctx)->tableName =
            qualifiedName();
        setState(339);
        match(PrestoSqlParser::RENAME);
        setState(340);
        match(PrestoSqlParser::COLUMN);
        setState(343);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 20, _ctx)) {
          case 1: {
            setState(341);
            match(PrestoSqlParser::IF);
            setState(342);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(345);
        antlrcpp::downCast<RenameColumnContext*>(_localctx)->from =
            identifier();
        setState(346);
        match(PrestoSqlParser::TO);
        setState(347);
        antlrcpp::downCast<RenameColumnContext*>(_localctx)->to = identifier();
        break;
      }

      case 15: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DropColumnContext>(
            _localctx);
        enterOuterAlt(_localctx, 15);
        setState(349);
        match(PrestoSqlParser::ALTER);
        setState(350);
        match(PrestoSqlParser::TABLE);
        setState(353);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 21, _ctx)) {
          case 1: {
            setState(351);
            match(PrestoSqlParser::IF);
            setState(352);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(355);
        antlrcpp::downCast<DropColumnContext*>(_localctx)->tableName =
            qualifiedName();
        setState(356);
        match(PrestoSqlParser::DROP);
        setState(357);
        match(PrestoSqlParser::COLUMN);
        setState(360);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 22, _ctx)) {
          case 1: {
            setState(358);
            match(PrestoSqlParser::IF);
            setState(359);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(362);
        antlrcpp::downCast<DropColumnContext*>(_localctx)->column =
            qualifiedName();
        break;
      }

      case 16: {
        _localctx = _tracker.createInstance<PrestoSqlParser::AddColumnContext>(
            _localctx);
        enterOuterAlt(_localctx, 16);
        setState(364);
        match(PrestoSqlParser::ALTER);
        setState(365);
        match(PrestoSqlParser::TABLE);
        setState(368);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 23, _ctx)) {
          case 1: {
            setState(366);
            match(PrestoSqlParser::IF);
            setState(367);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(370);
        antlrcpp::downCast<AddColumnContext*>(_localctx)->tableName =
            qualifiedName();
        setState(371);
        match(PrestoSqlParser::ADD);
        setState(372);
        match(PrestoSqlParser::COLUMN);
        setState(376);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 24, _ctx)) {
          case 1: {
            setState(373);
            match(PrestoSqlParser::IF);
            setState(374);
            match(PrestoSqlParser::NOT);
            setState(375);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(378);
        antlrcpp::downCast<AddColumnContext*>(_localctx)->column =
            columnDefinition();
        break;
      }

      case 17: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::AddConstraintContext>(
                _localctx);
        enterOuterAlt(_localctx, 17);
        setState(380);
        match(PrestoSqlParser::ALTER);
        setState(381);
        match(PrestoSqlParser::TABLE);
        setState(384);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 25, _ctx)) {
          case 1: {
            setState(382);
            match(PrestoSqlParser::IF);
            setState(383);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(386);
        antlrcpp::downCast<AddConstraintContext*>(_localctx)->tableName =
            qualifiedName();
        setState(387);
        match(PrestoSqlParser::ADD);
        setState(388);
        constraintSpecification();
        break;
      }

      case 18: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DropConstraintContext>(
                _localctx);
        enterOuterAlt(_localctx, 18);
        setState(390);
        match(PrestoSqlParser::ALTER);
        setState(391);
        match(PrestoSqlParser::TABLE);
        setState(394);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 26, _ctx)) {
          case 1: {
            setState(392);
            match(PrestoSqlParser::IF);
            setState(393);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(396);
        antlrcpp::downCast<DropConstraintContext*>(_localctx)->tableName =
            qualifiedName();
        setState(397);
        match(PrestoSqlParser::DROP);
        setState(398);
        match(PrestoSqlParser::CONSTRAINT);
        setState(401);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 27, _ctx)) {
          case 1: {
            setState(399);
            match(PrestoSqlParser::IF);
            setState(400);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(403);
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
        setState(405);
        match(PrestoSqlParser::ALTER);
        setState(406);
        match(PrestoSqlParser::TABLE);
        setState(409);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 28, _ctx)) {
          case 1: {
            setState(407);
            match(PrestoSqlParser::IF);
            setState(408);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(411);
        antlrcpp::downCast<AlterColumnSetNotNullContext*>(_localctx)
            ->tableName = qualifiedName();
        setState(412);
        match(PrestoSqlParser::ALTER);
        setState(414);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 29, _ctx)) {
          case 1: {
            setState(413);
            match(PrestoSqlParser::COLUMN);
            break;
          }

          default:
            break;
        }
        setState(416);
        antlrcpp::downCast<AlterColumnSetNotNullContext*>(_localctx)->column =
            identifier();
        setState(417);
        match(PrestoSqlParser::SET);
        setState(418);
        match(PrestoSqlParser::NOT);
        setState(419);
        match(PrestoSqlParser::NULL_LITERAL);
        break;
      }

      case 20: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::AlterColumnDropNotNullContext>(
                    _localctx);
        enterOuterAlt(_localctx, 20);
        setState(421);
        match(PrestoSqlParser::ALTER);
        setState(422);
        match(PrestoSqlParser::TABLE);
        setState(425);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 30, _ctx)) {
          case 1: {
            setState(423);
            match(PrestoSqlParser::IF);
            setState(424);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(427);
        antlrcpp::downCast<AlterColumnDropNotNullContext*>(_localctx)
            ->tableName = qualifiedName();
        setState(428);
        match(PrestoSqlParser::ALTER);
        setState(430);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 31, _ctx)) {
          case 1: {
            setState(429);
            match(PrestoSqlParser::COLUMN);
            break;
          }

          default:
            break;
        }
        setState(432);
        antlrcpp::downCast<AlterColumnDropNotNullContext*>(_localctx)->column =
            identifier();
        setState(433);
        match(PrestoSqlParser::DROP);
        setState(434);
        match(PrestoSqlParser::NOT);
        setState(435);
        match(PrestoSqlParser::NULL_LITERAL);
        break;
      }

      case 21: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SetTablePropertiesContext>(
                _localctx);
        enterOuterAlt(_localctx, 21);
        setState(437);
        match(PrestoSqlParser::ALTER);
        setState(438);
        match(PrestoSqlParser::TABLE);
        setState(441);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 32, _ctx)) {
          case 1: {
            setState(439);
            match(PrestoSqlParser::IF);
            setState(440);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(443);
        antlrcpp::downCast<SetTablePropertiesContext*>(_localctx)->tableName =
            qualifiedName();
        setState(444);
        match(PrestoSqlParser::SET);
        setState(445);
        match(PrestoSqlParser::PROPERTIES);
        setState(446);
        properties();
        break;
      }

      case 22: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::AnalyzeContext>(_localctx);
        enterOuterAlt(_localctx, 22);
        setState(448);
        match(PrestoSqlParser::ANALYZE);
        setState(449);
        qualifiedName();
        setState(452);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(450);
          match(PrestoSqlParser::WITH);
          setState(451);
          properties();
        }
        break;
      }

      case 23: {
        _localctx = _tracker.createInstance<PrestoSqlParser::CreateTypeContext>(
            _localctx);
        enterOuterAlt(_localctx, 23);
        setState(454);
        match(PrestoSqlParser::CREATE);
        setState(455);
        match(PrestoSqlParser::TYPE);
        setState(456);
        qualifiedName();
        setState(457);
        match(PrestoSqlParser::AS);
        setState(470);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case PrestoSqlParser::T__1: {
            setState(458);
            match(PrestoSqlParser::T__1);
            setState(459);
            sqlParameterDeclaration();
            setState(464);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == PrestoSqlParser::T__3) {
              setState(460);
              match(PrestoSqlParser::T__3);
              setState(461);
              sqlParameterDeclaration();
              setState(466);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
            setState(467);
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
          case PrestoSqlParser::EXCLUDE:
          case PrestoSqlParser::EXCLUDING:
          case PrestoSqlParser::EXECUTABLE:
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
          case PrestoSqlParser::GRAPH:
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
          case PrestoSqlParser::OPTIMIZED:
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
          case PrestoSqlParser::WINDOW:
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
            setState(469);
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
        setState(472);
        match(PrestoSqlParser::CREATE);
        setState(475);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::OR) {
          setState(473);
          match(PrestoSqlParser::OR);
          setState(474);
          match(PrestoSqlParser::REPLACE);
        }
        setState(477);
        match(PrestoSqlParser::VIEW);
        setState(478);
        qualifiedName();
        setState(481);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::SECURITY) {
          setState(479);
          match(PrestoSqlParser::SECURITY);
          setState(480);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::DEFINER

                || _la == PrestoSqlParser::INVOKER)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
        }
        setState(483);
        match(PrestoSqlParser::AS);
        setState(484);
        query();
        break;
      }

      case 25: {
        _localctx = _tracker.createInstance<PrestoSqlParser::RenameViewContext>(
            _localctx);
        enterOuterAlt(_localctx, 25);
        setState(486);
        match(PrestoSqlParser::ALTER);
        setState(487);
        match(PrestoSqlParser::VIEW);
        setState(490);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 38, _ctx)) {
          case 1: {
            setState(488);
            match(PrestoSqlParser::IF);
            setState(489);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(492);
        antlrcpp::downCast<RenameViewContext*>(_localctx)->from =
            qualifiedName();
        setState(493);
        match(PrestoSqlParser::RENAME);
        setState(494);
        match(PrestoSqlParser::TO);
        setState(495);
        antlrcpp::downCast<RenameViewContext*>(_localctx)->to = qualifiedName();
        break;
      }

      case 26: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DropViewContext>(
            _localctx);
        enterOuterAlt(_localctx, 26);
        setState(497);
        match(PrestoSqlParser::DROP);
        setState(498);
        match(PrestoSqlParser::VIEW);
        setState(501);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 39, _ctx)) {
          case 1: {
            setState(499);
            match(PrestoSqlParser::IF);
            setState(500);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(503);
        qualifiedName();
        break;
      }

      case 27: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::CreateMaterializedViewContext>(
                    _localctx);
        enterOuterAlt(_localctx, 27);
        setState(504);
        match(PrestoSqlParser::CREATE);
        setState(505);
        match(PrestoSqlParser::MATERIALIZED);
        setState(506);
        match(PrestoSqlParser::VIEW);
        setState(510);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 40, _ctx)) {
          case 1: {
            setState(507);
            match(PrestoSqlParser::IF);
            setState(508);
            match(PrestoSqlParser::NOT);
            setState(509);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(512);
        qualifiedName();
        setState(515);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::COMMENT) {
          setState(513);
          match(PrestoSqlParser::COMMENT);
          setState(514);
          string();
        }
        setState(519);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(517);
          match(PrestoSqlParser::WITH);
          setState(518);
          properties();
        }
        setState(521);
        match(PrestoSqlParser::AS);
        setState(527);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 43, _ctx)) {
          case 1: {
            setState(522);
            query();
            break;
          }

          case 2: {
            setState(523);
            match(PrestoSqlParser::T__1);
            setState(524);
            query();
            setState(525);
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
        setState(529);
        match(PrestoSqlParser::DROP);
        setState(530);
        match(PrestoSqlParser::MATERIALIZED);
        setState(531);
        match(PrestoSqlParser::VIEW);
        setState(534);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 44, _ctx)) {
          case 1: {
            setState(532);
            match(PrestoSqlParser::IF);
            setState(533);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(536);
        qualifiedName();
        break;
      }

      case 29: {
        _localctx = _tracker.createInstance<
            PrestoSqlParser::RefreshMaterializedViewContext>(_localctx);
        enterOuterAlt(_localctx, 29);
        setState(537);
        match(PrestoSqlParser::REFRESH);
        setState(538);
        match(PrestoSqlParser::MATERIALIZED);
        setState(539);
        match(PrestoSqlParser::VIEW);
        setState(540);
        qualifiedName();
        setState(541);
        match(PrestoSqlParser::WHERE);
        setState(542);
        booleanExpression(0);
        break;
      }

      case 30: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CreateFunctionContext>(
                _localctx);
        enterOuterAlt(_localctx, 30);
        setState(544);
        match(PrestoSqlParser::CREATE);
        setState(547);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::OR) {
          setState(545);
          match(PrestoSqlParser::OR);
          setState(546);
          match(PrestoSqlParser::REPLACE);
        }
        setState(550);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::TEMPORARY) {
          setState(549);
          match(PrestoSqlParser::TEMPORARY);
        }
        setState(552);
        match(PrestoSqlParser::FUNCTION);
        setState(553);
        antlrcpp::downCast<CreateFunctionContext*>(_localctx)->functionName =
            qualifiedName();
        setState(554);
        match(PrestoSqlParser::T__1);
        setState(563);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & -6508956968051886080) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -2335699536833519961) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -18163934272260209) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & 8646913422763212271) != 0)) {
          setState(555);
          sqlParameterDeclaration();
          setState(560);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(556);
            match(PrestoSqlParser::T__3);
            setState(557);
            sqlParameterDeclaration();
            setState(562);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(565);
        match(PrestoSqlParser::T__2);
        setState(566);
        match(PrestoSqlParser::RETURNS);
        setState(567);
        antlrcpp::downCast<CreateFunctionContext*>(_localctx)->returnType =
            type(0);
        setState(570);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::COMMENT) {
          setState(568);
          match(PrestoSqlParser::COMMENT);
          setState(569);
          string();
        }
        setState(572);
        routineCharacteristics();
        setState(573);
        routineBody();
        break;
      }

      case 31: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::AlterFunctionContext>(
                _localctx);
        enterOuterAlt(_localctx, 31);
        setState(575);
        match(PrestoSqlParser::ALTER);
        setState(576);
        match(PrestoSqlParser::FUNCTION);
        setState(577);
        qualifiedName();
        setState(579);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::T__1) {
          setState(578);
          types();
        }
        setState(581);
        alterRoutineCharacteristics();
        break;
      }

      case 32: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DropFunctionContext>(
                _localctx);
        enterOuterAlt(_localctx, 32);
        setState(583);
        match(PrestoSqlParser::DROP);
        setState(585);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::TEMPORARY) {
          setState(584);
          match(PrestoSqlParser::TEMPORARY);
        }
        setState(587);
        match(PrestoSqlParser::FUNCTION);
        setState(590);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 52, _ctx)) {
          case 1: {
            setState(588);
            match(PrestoSqlParser::IF);
            setState(589);
            match(PrestoSqlParser::EXISTS);
            break;
          }

          default:
            break;
        }
        setState(592);
        qualifiedName();
        setState(594);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::T__1) {
          setState(593);
          types();
        }
        break;
      }

      case 33: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CallContext>(_localctx);
        enterOuterAlt(_localctx, 33);
        setState(596);
        match(PrestoSqlParser::CALL);
        setState(597);
        qualifiedName();
        setState(598);
        match(PrestoSqlParser::T__1);
        setState(607);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & -6508829423092451292) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -2308677939035742217) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -18163934272260097) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -8582848577798673) != 0) ||
            _la == PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE

            || _la == PrestoSqlParser::DOUBLE_PRECISION) {
          setState(599);
          callArgument();
          setState(604);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(600);
            match(PrestoSqlParser::T__3);
            setState(601);
            callArgument();
            setState(606);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(609);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 34: {
        _localctx = _tracker.createInstance<PrestoSqlParser::CreateRoleContext>(
            _localctx);
        enterOuterAlt(_localctx, 34);
        setState(611);
        match(PrestoSqlParser::CREATE);
        setState(612);
        match(PrestoSqlParser::ROLE);
        setState(613);
        antlrcpp::downCast<CreateRoleContext*>(_localctx)->name = identifier();
        setState(617);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(614);
          match(PrestoSqlParser::WITH);
          setState(615);
          match(PrestoSqlParser::ADMIN);
          setState(616);
          grantor();
        }
        break;
      }

      case 35: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DropRoleContext>(
            _localctx);
        enterOuterAlt(_localctx, 35);
        setState(619);
        match(PrestoSqlParser::DROP);
        setState(620);
        match(PrestoSqlParser::ROLE);
        setState(621);
        antlrcpp::downCast<DropRoleContext*>(_localctx)->name = identifier();
        break;
      }

      case 36: {
        _localctx = _tracker.createInstance<PrestoSqlParser::GrantRolesContext>(
            _localctx);
        enterOuterAlt(_localctx, 36);
        setState(622);
        match(PrestoSqlParser::GRANT);
        setState(623);
        roles();
        setState(624);
        match(PrestoSqlParser::TO);
        setState(625);
        principal();
        setState(630);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(626);
          match(PrestoSqlParser::T__3);
          setState(627);
          principal();
          setState(632);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(636);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(633);
          match(PrestoSqlParser::WITH);
          setState(634);
          match(PrestoSqlParser::ADMIN);
          setState(635);
          match(PrestoSqlParser::OPTION);
        }
        setState(641);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::GRANTED) {
          setState(638);
          match(PrestoSqlParser::GRANTED);
          setState(639);
          match(PrestoSqlParser::BY);
          setState(640);
          grantor();
        }
        break;
      }

      case 37: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RevokeRolesContext>(
                _localctx);
        enterOuterAlt(_localctx, 37);
        setState(643);
        match(PrestoSqlParser::REVOKE);
        setState(647);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 60, _ctx)) {
          case 1: {
            setState(644);
            match(PrestoSqlParser::ADMIN);
            setState(645);
            match(PrestoSqlParser::OPTION);
            setState(646);
            match(PrestoSqlParser::FOR);
            break;
          }

          default:
            break;
        }
        setState(649);
        roles();
        setState(650);
        match(PrestoSqlParser::FROM);
        setState(651);
        principal();
        setState(656);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(652);
          match(PrestoSqlParser::T__3);
          setState(653);
          principal();
          setState(658);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(662);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::GRANTED) {
          setState(659);
          match(PrestoSqlParser::GRANTED);
          setState(660);
          match(PrestoSqlParser::BY);
          setState(661);
          grantor();
        }
        break;
      }

      case 38: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SetRoleContext>(_localctx);
        enterOuterAlt(_localctx, 38);
        setState(664);
        match(PrestoSqlParser::SET);
        setState(665);
        match(PrestoSqlParser::ROLE);
        setState(669);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 63, _ctx)) {
          case 1: {
            setState(666);
            match(PrestoSqlParser::ALL);
            break;
          }

          case 2: {
            setState(667);
            match(PrestoSqlParser::NONE);
            break;
          }

          case 3: {
            setState(668);
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
        setState(671);
        match(PrestoSqlParser::GRANT);
        setState(682);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 65, _ctx)) {
          case 1: {
            setState(672);
            privilege();
            setState(677);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == PrestoSqlParser::T__3) {
              setState(673);
              match(PrestoSqlParser::T__3);
              setState(674);
              privilege();
              setState(679);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
            break;
          }

          case 2: {
            setState(680);
            match(PrestoSqlParser::ALL);
            setState(681);
            match(PrestoSqlParser::PRIVILEGES);
            break;
          }

          default:
            break;
        }
        setState(684);
        match(PrestoSqlParser::ON);
        setState(686);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::TABLE) {
          setState(685);
          match(PrestoSqlParser::TABLE);
        }
        setState(688);
        qualifiedName();
        setState(689);
        match(PrestoSqlParser::TO);
        setState(690);
        antlrcpp::downCast<GrantContext*>(_localctx)->grantee = principal();
        setState(694);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(691);
          match(PrestoSqlParser::WITH);
          setState(692);
          match(PrestoSqlParser::GRANT);
          setState(693);
          match(PrestoSqlParser::OPTION);
        }
        break;
      }

      case 40: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RevokeContext>(_localctx);
        enterOuterAlt(_localctx, 40);
        setState(696);
        match(PrestoSqlParser::REVOKE);
        setState(700);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 68, _ctx)) {
          case 1: {
            setState(697);
            match(PrestoSqlParser::GRANT);
            setState(698);
            match(PrestoSqlParser::OPTION);
            setState(699);
            match(PrestoSqlParser::FOR);
            break;
          }

          default:
            break;
        }
        setState(712);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 70, _ctx)) {
          case 1: {
            setState(702);
            privilege();
            setState(707);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == PrestoSqlParser::T__3) {
              setState(703);
              match(PrestoSqlParser::T__3);
              setState(704);
              privilege();
              setState(709);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
            break;
          }

          case 2: {
            setState(710);
            match(PrestoSqlParser::ALL);
            setState(711);
            match(PrestoSqlParser::PRIVILEGES);
            break;
          }

          default:
            break;
        }
        setState(714);
        match(PrestoSqlParser::ON);
        setState(716);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::TABLE) {
          setState(715);
          match(PrestoSqlParser::TABLE);
        }
        setState(718);
        qualifiedName();
        setState(719);
        match(PrestoSqlParser::FROM);
        setState(720);
        antlrcpp::downCast<RevokeContext*>(_localctx)->grantee = principal();
        break;
      }

      case 41: {
        _localctx = _tracker.createInstance<PrestoSqlParser::ShowGrantsContext>(
            _localctx);
        enterOuterAlt(_localctx, 41);
        setState(722);
        match(PrestoSqlParser::SHOW);
        setState(723);
        match(PrestoSqlParser::GRANTS);
        setState(729);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::ON) {
          setState(724);
          match(PrestoSqlParser::ON);
          setState(726);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::TABLE) {
            setState(725);
            match(PrestoSqlParser::TABLE);
          }
          setState(728);
          qualifiedName();
        }
        break;
      }

      case 42: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ExplainContext>(_localctx);
        enterOuterAlt(_localctx, 42);
        setState(731);
        match(PrestoSqlParser::EXPLAIN);
        setState(733);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 74, _ctx)) {
          case 1: {
            setState(732);
            match(PrestoSqlParser::ANALYZE);
            break;
          }

          default:
            break;
        }
        setState(736);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::VERBOSE) {
          setState(735);
          match(PrestoSqlParser::VERBOSE);
        }
        setState(749);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 77, _ctx)) {
          case 1: {
            setState(738);
            match(PrestoSqlParser::T__1);
            setState(739);
            explainOption();
            setState(744);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == PrestoSqlParser::T__3) {
              setState(740);
              match(PrestoSqlParser::T__3);
              setState(741);
              explainOption();
              setState(746);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
            setState(747);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        setState(751);
        statement();
        break;
      }

      case 43: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowCreateTableContext>(
                _localctx);
        enterOuterAlt(_localctx, 43);
        setState(752);
        match(PrestoSqlParser::SHOW);
        setState(753);
        match(PrestoSqlParser::CREATE);
        setState(754);
        match(PrestoSqlParser::TABLE);
        setState(755);
        qualifiedName();
        break;
      }

      case 44: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowCreateViewContext>(
                _localctx);
        enterOuterAlt(_localctx, 44);
        setState(756);
        match(PrestoSqlParser::SHOW);
        setState(757);
        match(PrestoSqlParser::CREATE);
        setState(758);
        match(PrestoSqlParser::VIEW);
        setState(759);
        qualifiedName();
        break;
      }

      case 45: {
        _localctx = _tracker.createInstance<
            PrestoSqlParser::ShowCreateMaterializedViewContext>(_localctx);
        enterOuterAlt(_localctx, 45);
        setState(760);
        match(PrestoSqlParser::SHOW);
        setState(761);
        match(PrestoSqlParser::CREATE);
        setState(762);
        match(PrestoSqlParser::MATERIALIZED);
        setState(763);
        match(PrestoSqlParser::VIEW);
        setState(764);
        qualifiedName();
        break;
      }

      case 46: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowCreateFunctionContext>(
                _localctx);
        enterOuterAlt(_localctx, 46);
        setState(765);
        match(PrestoSqlParser::SHOW);
        setState(766);
        match(PrestoSqlParser::CREATE);
        setState(767);
        match(PrestoSqlParser::FUNCTION);
        setState(768);
        qualifiedName();
        setState(770);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::T__1) {
          setState(769);
          types();
        }
        break;
      }

      case 47: {
        _localctx = _tracker.createInstance<PrestoSqlParser::ShowTablesContext>(
            _localctx);
        enterOuterAlt(_localctx, 47);
        setState(772);
        match(PrestoSqlParser::SHOW);
        setState(773);
        match(PrestoSqlParser::TABLES);
        setState(776);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::FROM

            || _la == PrestoSqlParser::IN) {
          setState(774);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::FROM

                || _la == PrestoSqlParser::IN)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(775);
          qualifiedName();
        }
        setState(784);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::LIKE) {
          setState(778);
          match(PrestoSqlParser::LIKE);
          setState(779);
          antlrcpp::downCast<ShowTablesContext*>(_localctx)->pattern = string();
          setState(782);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::ESCAPE) {
            setState(780);
            match(PrestoSqlParser::ESCAPE);
            setState(781);
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
        setState(786);
        match(PrestoSqlParser::SHOW);
        setState(787);
        match(PrestoSqlParser::SCHEMAS);
        setState(790);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::FROM

            || _la == PrestoSqlParser::IN) {
          setState(788);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::FROM

                || _la == PrestoSqlParser::IN)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(789);
          identifier();
        }
        setState(798);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::LIKE) {
          setState(792);
          match(PrestoSqlParser::LIKE);
          setState(793);
          antlrcpp::downCast<ShowSchemasContext*>(_localctx)->pattern =
              string();
          setState(796);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::ESCAPE) {
            setState(794);
            match(PrestoSqlParser::ESCAPE);
            setState(795);
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
        setState(800);
        match(PrestoSqlParser::SHOW);
        setState(801);
        match(PrestoSqlParser::CATALOGS);
        setState(808);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::LIKE) {
          setState(802);
          match(PrestoSqlParser::LIKE);
          setState(803);
          antlrcpp::downCast<ShowCatalogsContext*>(_localctx)->pattern =
              string();
          setState(806);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::ESCAPE) {
            setState(804);
            match(PrestoSqlParser::ESCAPE);
            setState(805);
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
        setState(810);
        match(PrestoSqlParser::SHOW);
        setState(811);
        match(PrestoSqlParser::COLUMNS);
        setState(812);
        _la = _input->LA(1);
        if (!(_la == PrestoSqlParser::FROM

              || _la == PrestoSqlParser::IN)) {
          _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(813);
        qualifiedName();
        break;
      }

      case 51: {
        _localctx = _tracker.createInstance<PrestoSqlParser::ShowStatsContext>(
            _localctx);
        enterOuterAlt(_localctx, 51);
        setState(814);
        match(PrestoSqlParser::SHOW);
        setState(815);
        match(PrestoSqlParser::STATS);
        setState(816);
        match(PrestoSqlParser::FOR);
        setState(817);
        qualifiedName();
        break;
      }

      case 52: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowStatsForQueryContext>(
                _localctx);
        enterOuterAlt(_localctx, 52);
        setState(818);
        match(PrestoSqlParser::SHOW);
        setState(819);
        match(PrestoSqlParser::STATS);
        setState(820);
        match(PrestoSqlParser::FOR);
        setState(821);
        match(PrestoSqlParser::T__1);
        setState(822);
        querySpecification();
        setState(823);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 53: {
        _localctx = _tracker.createInstance<PrestoSqlParser::ShowRolesContext>(
            _localctx);
        enterOuterAlt(_localctx, 53);
        setState(825);
        match(PrestoSqlParser::SHOW);
        setState(827);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::CURRENT) {
          setState(826);
          match(PrestoSqlParser::CURRENT);
        }
        setState(829);
        match(PrestoSqlParser::ROLES);
        setState(832);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::FROM

            || _la == PrestoSqlParser::IN) {
          setState(830);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::FROM

                || _la == PrestoSqlParser::IN)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(831);
          identifier();
        }
        break;
      }

      case 54: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowRoleGrantsContext>(
                _localctx);
        enterOuterAlt(_localctx, 54);
        setState(834);
        match(PrestoSqlParser::SHOW);
        setState(835);
        match(PrestoSqlParser::ROLE);
        setState(836);
        match(PrestoSqlParser::GRANTS);
        setState(839);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::FROM

            || _la == PrestoSqlParser::IN) {
          setState(837);
          _la = _input->LA(1);
          if (!(_la == PrestoSqlParser::FROM

                || _la == PrestoSqlParser::IN)) {
            _errHandler->recoverInline(this);
          } else {
            _errHandler->reportMatch(this);
            consume();
          }
          setState(838);
          identifier();
        }
        break;
      }

      case 55: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowColumnsContext>(
                _localctx);
        enterOuterAlt(_localctx, 55);
        setState(841);
        match(PrestoSqlParser::DESCRIBE);
        setState(842);
        qualifiedName();
        break;
      }

      case 56: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowColumnsContext>(
                _localctx);
        enterOuterAlt(_localctx, 56);
        setState(843);
        match(PrestoSqlParser::DESC);
        setState(844);
        qualifiedName();
        break;
      }

      case 57: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ShowFunctionsContext>(
                _localctx);
        enterOuterAlt(_localctx, 57);
        setState(845);
        match(PrestoSqlParser::SHOW);
        setState(846);
        match(PrestoSqlParser::FUNCTIONS);
        setState(853);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::LIKE) {
          setState(847);
          match(PrestoSqlParser::LIKE);
          setState(848);
          antlrcpp::downCast<ShowFunctionsContext*>(_localctx)->pattern =
              string();
          setState(851);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::ESCAPE) {
            setState(849);
            match(PrestoSqlParser::ESCAPE);
            setState(850);
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
        setState(855);
        match(PrestoSqlParser::SHOW);
        setState(856);
        match(PrestoSqlParser::SESSION);
        setState(863);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::LIKE) {
          setState(857);
          match(PrestoSqlParser::LIKE);
          setState(858);
          antlrcpp::downCast<ShowSessionContext*>(_localctx)->pattern =
              string();
          setState(861);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == PrestoSqlParser::ESCAPE) {
            setState(859);
            match(PrestoSqlParser::ESCAPE);
            setState(860);
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
        setState(865);
        match(PrestoSqlParser::SET);
        setState(866);
        match(PrestoSqlParser::SESSION);
        setState(867);
        qualifiedName();
        setState(868);
        match(PrestoSqlParser::EQ);
        setState(869);
        expression();
        break;
      }

      case 60: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ResetSessionContext>(
                _localctx);
        enterOuterAlt(_localctx, 60);
        setState(871);
        match(PrestoSqlParser::RESET);
        setState(872);
        match(PrestoSqlParser::SESSION);
        setState(873);
        qualifiedName();
        break;
      }

      case 61: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::StartTransactionContext>(
                _localctx);
        enterOuterAlt(_localctx, 61);
        setState(874);
        match(PrestoSqlParser::START);
        setState(875);
        match(PrestoSqlParser::TRANSACTION);
        setState(884);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::ISOLATION

            || _la == PrestoSqlParser::READ) {
          setState(876);
          transactionMode();
          setState(881);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(877);
            match(PrestoSqlParser::T__3);
            setState(878);
            transactionMode();
            setState(883);
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
        setState(886);
        match(PrestoSqlParser::COMMIT);
        setState(888);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WORK) {
          setState(887);
          match(PrestoSqlParser::WORK);
        }
        break;
      }

      case 63: {
        _localctx = _tracker.createInstance<PrestoSqlParser::RollbackContext>(
            _localctx);
        enterOuterAlt(_localctx, 63);
        setState(890);
        match(PrestoSqlParser::ROLLBACK);
        setState(892);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WORK) {
          setState(891);
          match(PrestoSqlParser::WORK);
        }
        break;
      }

      case 64: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::PrepareContext>(_localctx);
        enterOuterAlt(_localctx, 64);
        setState(894);
        match(PrestoSqlParser::PREPARE);
        setState(895);
        identifier();
        setState(896);
        match(PrestoSqlParser::FROM);
        setState(897);
        statement();
        break;
      }

      case 65: {
        _localctx = _tracker.createInstance<PrestoSqlParser::DeallocateContext>(
            _localctx);
        enterOuterAlt(_localctx, 65);
        setState(899);
        match(PrestoSqlParser::DEALLOCATE);
        setState(900);
        match(PrestoSqlParser::PREPARE);
        setState(901);
        identifier();
        break;
      }

      case 66: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ExecuteContext>(_localctx);
        enterOuterAlt(_localctx, 66);
        setState(902);
        match(PrestoSqlParser::EXECUTE);
        setState(903);
        identifier();
        setState(913);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::USING) {
          setState(904);
          match(PrestoSqlParser::USING);
          setState(905);
          expression();
          setState(910);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(906);
            match(PrestoSqlParser::T__3);
            setState(907);
            expression();
            setState(912);
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
        setState(915);
        match(PrestoSqlParser::DESCRIBE);
        setState(916);
        match(PrestoSqlParser::INPUT);
        setState(917);
        identifier();
        break;
      }

      case 68: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DescribeOutputContext>(
                _localctx);
        enterOuterAlt(_localctx, 68);
        setState(918);
        match(PrestoSqlParser::DESCRIBE);
        setState(919);
        match(PrestoSqlParser::OUTPUT);
        setState(920);
        identifier();
        break;
      }

      case 69: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UpdateContext>(_localctx);
        enterOuterAlt(_localctx, 69);
        setState(921);
        match(PrestoSqlParser::UPDATE);
        setState(922);
        qualifiedName();
        setState(923);
        match(PrestoSqlParser::SET);
        setState(924);
        updateAssignment();
        setState(929);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(925);
          match(PrestoSqlParser::T__3);
          setState(926);
          updateAssignment();
          setState(931);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(934);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WHERE) {
          setState(932);
          match(PrestoSqlParser::WHERE);
          setState(933);
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

std::any PrestoSqlParser::QueryContext::accept(
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
    setState(939);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::WITH) {
      setState(938);
      with();
    }
    setState(941);
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

std::any PrestoSqlParser::WithContext::accept(tree::ParseTreeVisitor* visitor) {
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
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(943);
    match(PrestoSqlParser::WITH);
    setState(945);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::RECURSIVE) {
      setState(944);
      match(PrestoSqlParser::RECURSIVE);
    }
    setState(947);
    namedQuery();
    setState(952);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 105, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(948);
        match(PrestoSqlParser::T__3);
        setState(949);
        namedQuery();
      }
      setState(954);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 105, _ctx);
    }
    setState(956);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::T__3) {
      setState(955);
      match(PrestoSqlParser::T__3);
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

std::any PrestoSqlParser::TableElementContext::accept(
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
    setState(961);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 107, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(958);
        constraintSpecification();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(959);
        columnDefinition();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(960);
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

std::any PrestoSqlParser::ColumnDefinitionContext::accept(
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
    setState(963);
    identifier();
    setState(964);
    type(0);
    setState(967);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::NOT) {
      setState(965);
      match(PrestoSqlParser::NOT);
      setState(966);
      match(PrestoSqlParser::NULL_LITERAL);
    }
    setState(971);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::COMMENT) {
      setState(969);
      match(PrestoSqlParser::COMMENT);
      setState(970);
      string();
    }
    setState(975);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::WITH) {
      setState(973);
      match(PrestoSqlParser::WITH);
      setState(974);
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

std::any PrestoSqlParser::LikeClauseContext::accept(
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
    setState(977);
    match(PrestoSqlParser::LIKE);
    setState(978);
    qualifiedName();
    setState(981);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::EXCLUDING

        || _la == PrestoSqlParser::INCLUDING) {
      setState(979);
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
      setState(980);
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

std::any PrestoSqlParser::PropertiesContext::accept(
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
    setState(983);
    match(PrestoSqlParser::T__1);
    setState(984);
    property();
    setState(989);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::T__3) {
      setState(985);
      match(PrestoSqlParser::T__3);
      setState(986);
      property();
      setState(991);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(992);
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

std::any PrestoSqlParser::PropertyContext::accept(
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
    setState(994);
    identifier();
    setState(995);
    match(PrestoSqlParser::EQ);
    setState(996);
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

std::any PrestoSqlParser::SqlParameterDeclarationContext::accept(
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
    setState(998);
    identifier();
    setState(999);
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

std::any PrestoSqlParser::RoutineCharacteristicsContext::accept(
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
    setState(1004);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::CALLED

           || _la == PrestoSqlParser::DETERMINISTIC ||
           ((((_la - 112) & ~0x3fULL) == 0) &&
            ((1ULL << (_la - 112)) & 576460752311812097) != 0)) {
      setState(1001);
      routineCharacteristic();
      setState(1006);
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

std::any PrestoSqlParser::RoutineCharacteristicContext::accept(
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
    setState(1011);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::LANGUAGE: {
        enterOuterAlt(_localctx, 1);
        setState(1007);
        match(PrestoSqlParser::LANGUAGE);
        setState(1008);
        language();
        break;
      }

      case PrestoSqlParser::DETERMINISTIC:
      case PrestoSqlParser::NOT: {
        enterOuterAlt(_localctx, 2);
        setState(1009);
        determinism();
        break;
      }

      case PrestoSqlParser::CALLED:
      case PrestoSqlParser::RETURNS: {
        enterOuterAlt(_localctx, 3);
        setState(1010);
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

std::any PrestoSqlParser::AlterRoutineCharacteristicsContext::accept(
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
    setState(1016);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::CALLED || _la == PrestoSqlParser::RETURNS) {
      setState(1013);
      alterRoutineCharacteristic();
      setState(1018);
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

std::any PrestoSqlParser::AlterRoutineCharacteristicContext::accept(
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
    setState(1019);
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

std::any PrestoSqlParser::RoutineBodyContext::accept(
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
    setState(1023);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::RETURN: {
        enterOuterAlt(_localctx, 1);
        setState(1021);
        returnStatement();
        break;
      }

      case PrestoSqlParser::EXTERNAL: {
        enterOuterAlt(_localctx, 2);
        setState(1022);
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

std::any PrestoSqlParser::ReturnStatementContext::accept(
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
    setState(1025);
    match(PrestoSqlParser::RETURN);
    setState(1026);
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

std::any PrestoSqlParser::ExternalBodyReferenceContext::accept(
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
    setState(1028);
    match(PrestoSqlParser::EXTERNAL);
    setState(1031);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::NAME) {
      setState(1029);
      match(PrestoSqlParser::NAME);
      setState(1030);
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

std::any PrestoSqlParser::LanguageContext::accept(
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
    setState(1035);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 118, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(1033);
        match(PrestoSqlParser::SQL);
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(1034);
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

std::any PrestoSqlParser::DeterminismContext::accept(
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
    setState(1040);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::DETERMINISTIC: {
        enterOuterAlt(_localctx, 1);
        setState(1037);
        match(PrestoSqlParser::DETERMINISTIC);
        break;
      }

      case PrestoSqlParser::NOT: {
        enterOuterAlt(_localctx, 2);
        setState(1038);
        match(PrestoSqlParser::NOT);
        setState(1039);
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

std::any PrestoSqlParser::NullCallClauseContext::accept(
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
    setState(1051);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::RETURNS: {
        enterOuterAlt(_localctx, 1);
        setState(1042);
        match(PrestoSqlParser::RETURNS);
        setState(1043);
        match(PrestoSqlParser::NULL_LITERAL);
        setState(1044);
        match(PrestoSqlParser::ON);
        setState(1045);
        match(PrestoSqlParser::NULL_LITERAL);
        setState(1046);
        match(PrestoSqlParser::INPUT);
        break;
      }

      case PrestoSqlParser::CALLED: {
        enterOuterAlt(_localctx, 2);
        setState(1047);
        match(PrestoSqlParser::CALLED);
        setState(1048);
        match(PrestoSqlParser::ON);
        setState(1049);
        match(PrestoSqlParser::NULL_LITERAL);
        setState(1050);
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

std::any PrestoSqlParser::ExternalRoutineNameContext::accept(
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
    setState(1053);
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

std::any PrestoSqlParser::QueryNoWithContext::accept(
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
    setState(1055);
    queryTerm(0);
    setState(1066);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::ORDER) {
      setState(1056);
      match(PrestoSqlParser::ORDER);
      setState(1057);
      match(PrestoSqlParser::BY);
      setState(1058);
      sortItem();
      setState(1063);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PrestoSqlParser::T__3) {
        setState(1059);
        match(PrestoSqlParser::T__3);
        setState(1060);
        sortItem();
        setState(1065);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1073);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::OFFSET) {
      setState(1068);
      match(PrestoSqlParser::OFFSET);
      setState(1069);
      antlrcpp::downCast<QueryNoWithContext*>(_localctx)->offset =
          match(PrestoSqlParser::INTEGER_VALUE);
      setState(1071);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == PrestoSqlParser::ROW

          || _la == PrestoSqlParser::ROWS) {
        setState(1070);
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
    setState(1084);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::FETCH

        || _la == PrestoSqlParser::LIMIT) {
      setState(1082);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case PrestoSqlParser::LIMIT: {
          setState(1075);
          match(PrestoSqlParser::LIMIT);
          setState(1076);
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
          setState(1077);
          match(PrestoSqlParser::FETCH);
          setState(1078);
          match(PrestoSqlParser::FIRST);
          setState(1079);
          antlrcpp::downCast<QueryNoWithContext*>(_localctx)->fetchFirstNRows =
              match(PrestoSqlParser::INTEGER_VALUE);
          setState(1080);
          match(PrestoSqlParser::ROWS);
          setState(1081);
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

std::any PrestoSqlParser::QueryTermDefaultContext::accept(
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

std::any PrestoSqlParser::SetOperationContext::accept(
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

    setState(1087);
    queryPrimary();
    _ctx->stop = _input->LT(-1);
    setState(1103);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 130, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1101);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 129, _ctx)) {
          case 1: {
            auto newContext = _tracker.createInstance<SetOperationContext>(
                _tracker.createInstance<QueryTermContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->left = previousContext;
            pushNewRecursionContext(newContext, startState, RuleQueryTerm);
            setState(1089);

            if (!(precpred(_ctx, 2)))
              throw FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(1090);
            antlrcpp::downCast<SetOperationContext*>(_localctx)->op =
                match(PrestoSqlParser::INTERSECT);
            setState(1092);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == PrestoSqlParser::ALL

                || _la == PrestoSqlParser::DISTINCT) {
              setState(1091);
              setQuantifier();
            }
            setState(1094);
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
            setState(1095);

            if (!(precpred(_ctx, 1)))
              throw FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(1096);
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
            setState(1098);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == PrestoSqlParser::ALL

                || _la == PrestoSqlParser::DISTINCT) {
              setState(1097);
              setQuantifier();
            }
            setState(1100);
            antlrcpp::downCast<SetOperationContext*>(_localctx)->right =
                queryTerm(2);
            break;
          }

          default:
            break;
        }
      }
      setState(1105);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 130, _ctx);
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

std::any PrestoSqlParser::SubqueryContext::accept(
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

std::any PrestoSqlParser::QueryPrimaryDefaultContext::accept(
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

std::any PrestoSqlParser::TableContext::accept(
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

std::any PrestoSqlParser::InlineTableContext::accept(
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
    setState(1125);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::FROM:
      case PrestoSqlParser::SELECT: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::QueryPrimaryDefaultContext>(
                    _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1106);
        querySpecification();
        break;
      }

      case PrestoSqlParser::TABLE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TableContext>(_localctx);
        enterOuterAlt(_localctx, 2);
        setState(1107);
        match(PrestoSqlParser::TABLE);
        setState(1108);
        qualifiedName();
        break;
      }

      case PrestoSqlParser::VALUES: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::InlineTableContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(1109);
        match(PrestoSqlParser::VALUES);
        setState(1110);
        expression();
        setState(1115);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 131, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(1111);
            match(PrestoSqlParser::T__3);
            setState(1112);
            expression();
          }
          setState(1117);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 131, _ctx);
        }
        setState(1119);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 132, _ctx)) {
          case 1: {
            setState(1118);
            match(PrestoSqlParser::T__3);
            break;
          }

          default:
            break;
        }
        break;
      }

      case PrestoSqlParser::T__1: {
        _localctx = _tracker.createInstance<PrestoSqlParser::SubqueryContext>(
            _localctx);
        enterOuterAlt(_localctx, 4);
        setState(1121);
        match(PrestoSqlParser::T__1);
        setState(1122);
        queryNoWith();
        setState(1123);
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

std::any PrestoSqlParser::SortItemContext::accept(
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
    setState(1127);
    expression();
    setState(1129);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::ASC

        || _la == PrestoSqlParser::DESC) {
      setState(1128);
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
    setState(1133);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::NULLS) {
      setState(1131);
      match(PrestoSqlParser::NULLS);
      setState(1132);
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

tree::TerminalNode* PrestoSqlParser::QuerySpecificationContext::WINDOW() {
  return getToken(PrestoSqlParser::WINDOW, 0);
}

std::vector<PrestoSqlParser::WindowDefinitionContext*>
PrestoSqlParser::QuerySpecificationContext::windowDefinition() {
  return getRuleContexts<PrestoSqlParser::WindowDefinitionContext>();
}

PrestoSqlParser::WindowDefinitionContext*
PrestoSqlParser::QuerySpecificationContext::windowDefinition(size_t i) {
  return getRuleContext<PrestoSqlParser::WindowDefinitionContext>(i);
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

std::any PrestoSqlParser::QuerySpecificationContext::accept(
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
    setState(1218);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::SELECT: {
        enterOuterAlt(_localctx, 1);
        setState(1135);
        match(PrestoSqlParser::SELECT);
        setState(1137);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 136, _ctx)) {
          case 1: {
            setState(1136);
            setQuantifier();
            break;
          }

          default:
            break;
        }
        setState(1139);
        selectItem();
        setState(1144);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 137, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(1140);
            match(PrestoSqlParser::T__3);
            setState(1141);
            selectItem();
          }
          setState(1146);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 137, _ctx);
        }
        setState(1148);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 138, _ctx)) {
          case 1: {
            setState(1147);
            match(PrestoSqlParser::T__3);
            break;
          }

          default:
            break;
        }
        setState(1159);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 140, _ctx)) {
          case 1: {
            setState(1150);
            match(PrestoSqlParser::FROM);
            setState(1151);
            relation(0);
            setState(1156);
            _errHandler->sync(this);
            alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
                _input, 139, _ctx);
            while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
              if (alt == 1) {
                setState(1152);
                match(PrestoSqlParser::T__3);
                setState(1153);
                relation(0);
              }
              setState(1158);
              _errHandler->sync(this);
              alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
                  _input, 139, _ctx);
            }
            break;
          }

          default:
            break;
        }
        setState(1163);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 141, _ctx)) {
          case 1: {
            setState(1161);
            match(PrestoSqlParser::WHERE);
            setState(1162);
            antlrcpp::downCast<QuerySpecificationContext*>(_localctx)->where =
                booleanExpression(0);
            break;
          }

          default:
            break;
        }
        setState(1168);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 142, _ctx)) {
          case 1: {
            setState(1165);
            match(PrestoSqlParser::GROUP);
            setState(1166);
            match(PrestoSqlParser::BY);
            setState(1167);
            groupBy();
            break;
          }

          default:
            break;
        }
        setState(1172);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 143, _ctx)) {
          case 1: {
            setState(1170);
            match(PrestoSqlParser::HAVING);
            setState(1171);
            antlrcpp::downCast<QuerySpecificationContext*>(_localctx)->having =
                booleanExpression(0);
            break;
          }

          default:
            break;
        }
        setState(1183);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 145, _ctx)) {
          case 1: {
            setState(1174);
            match(PrestoSqlParser::WINDOW);
            setState(1175);
            windowDefinition();
            setState(1180);
            _errHandler->sync(this);
            alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
                _input, 144, _ctx);
            while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
              if (alt == 1) {
                setState(1176);
                match(PrestoSqlParser::T__3);
                setState(1177);
                windowDefinition();
              }
              setState(1182);
              _errHandler->sync(this);
              alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
                  _input, 144, _ctx);
            }
            break;
          }

          default:
            break;
        }
        break;
      }

      case PrestoSqlParser::FROM: {
        enterOuterAlt(_localctx, 2);
        setState(1185);
        match(PrestoSqlParser::FROM);
        setState(1186);
        relation(0);
        setState(1191);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 146, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(1187);
            match(PrestoSqlParser::T__3);
            setState(1188);
            relation(0);
          }
          setState(1193);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 146, _ctx);
        }
        setState(1196);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 147, _ctx)) {
          case 1: {
            setState(1194);
            match(PrestoSqlParser::WHERE);
            setState(1195);
            antlrcpp::downCast<QuerySpecificationContext*>(_localctx)->where =
                booleanExpression(0);
            break;
          }

          default:
            break;
        }
        setState(1201);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 148, _ctx)) {
          case 1: {
            setState(1198);
            match(PrestoSqlParser::GROUP);
            setState(1199);
            match(PrestoSqlParser::BY);
            setState(1200);
            groupBy();
            break;
          }

          default:
            break;
        }
        setState(1205);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 149, _ctx)) {
          case 1: {
            setState(1203);
            match(PrestoSqlParser::HAVING);
            setState(1204);
            antlrcpp::downCast<QuerySpecificationContext*>(_localctx)->having =
                booleanExpression(0);
            break;
          }

          default:
            break;
        }
        setState(1216);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 151, _ctx)) {
          case 1: {
            setState(1207);
            match(PrestoSqlParser::WINDOW);
            setState(1208);
            windowDefinition();
            setState(1213);
            _errHandler->sync(this);
            alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
                _input, 150, _ctx);
            while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
              if (alt == 1) {
                setState(1209);
                match(PrestoSqlParser::T__3);
                setState(1210);
                windowDefinition();
              }
              setState(1215);
              _errHandler->sync(this);
              alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
                  _input, 150, _ctx);
            }
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

//----------------- WindowDefinitionContext
//------------------------------------------------------------------

PrestoSqlParser::WindowDefinitionContext::WindowDefinitionContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::WindowDefinitionContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

PrestoSqlParser::WindowSpecificationContext*
PrestoSqlParser::WindowDefinitionContext::windowSpecification() {
  return getRuleContext<PrestoSqlParser::WindowSpecificationContext>(0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::WindowDefinitionContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

size_t PrestoSqlParser::WindowDefinitionContext::getRuleIndex() const {
  return PrestoSqlParser::RuleWindowDefinition;
}

void PrestoSqlParser::WindowDefinitionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterWindowDefinition(this);
}

void PrestoSqlParser::WindowDefinitionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitWindowDefinition(this);
}

std::any PrestoSqlParser::WindowDefinitionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitWindowDefinition(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::WindowDefinitionContext* PrestoSqlParser::windowDefinition() {
  WindowDefinitionContext* _localctx =
      _tracker.createInstance<WindowDefinitionContext>(_ctx, getState());
  enterRule(_localctx, 56, PrestoSqlParser::RuleWindowDefinition);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1220);
    antlrcpp::downCast<WindowDefinitionContext*>(_localctx)->name =
        identifier();
    setState(1221);
    match(PrestoSqlParser::AS);
    setState(1222);
    match(PrestoSqlParser::T__1);
    setState(1223);
    windowSpecification();
    setState(1224);
    match(PrestoSqlParser::T__2);

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

std::any PrestoSqlParser::GroupByContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitGroupBy(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::GroupByContext* PrestoSqlParser::groupBy() {
  GroupByContext* _localctx =
      _tracker.createInstance<GroupByContext>(_ctx, getState());
  enterRule(_localctx, 58, PrestoSqlParser::RuleGroupBy);

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
    setState(1227);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 153, _ctx)) {
      case 1: {
        setState(1226);
        setQuantifier();
        break;
      }

      default:
        break;
    }
    setState(1229);
    groupingElement();
    setState(1234);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 154, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1230);
        match(PrestoSqlParser::T__3);
        setState(1231);
        groupingElement();
      }
      setState(1236);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 154, _ctx);
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

std::any PrestoSqlParser::MultipleGroupingSetsContext::accept(
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

std::any PrestoSqlParser::SingleGroupingSetContext::accept(
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

std::any PrestoSqlParser::CubeContext::accept(tree::ParseTreeVisitor* visitor) {
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

std::any PrestoSqlParser::RollupContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRollup(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::GroupingElementContext* PrestoSqlParser::groupingElement() {
  GroupingElementContext* _localctx =
      _tracker.createInstance<GroupingElementContext>(_ctx, getState());
  enterRule(_localctx, 60, PrestoSqlParser::RuleGroupingElement);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1277);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 160, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SingleGroupingSetContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1237);
        groupingSet();
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RollupContext>(_localctx);
        enterOuterAlt(_localctx, 2);
        setState(1238);
        match(PrestoSqlParser::ROLLUP);
        setState(1239);
        match(PrestoSqlParser::T__1);
        setState(1248);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & -6508829423092451292) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -2308677939035742217) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -18163934272260097) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -8582848577798673) != 0) ||
            _la == PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE

            || _la == PrestoSqlParser::DOUBLE_PRECISION) {
          setState(1240);
          expression();
          setState(1245);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1241);
            match(PrestoSqlParser::T__3);
            setState(1242);
            expression();
            setState(1247);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1250);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CubeContext>(_localctx);
        enterOuterAlt(_localctx, 3);
        setState(1251);
        match(PrestoSqlParser::CUBE);
        setState(1252);
        match(PrestoSqlParser::T__1);
        setState(1261);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & -6508829423092451292) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -2308677939035742217) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -18163934272260097) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -8582848577798673) != 0) ||
            _la == PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE

            || _la == PrestoSqlParser::DOUBLE_PRECISION) {
          setState(1253);
          expression();
          setState(1258);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1254);
            match(PrestoSqlParser::T__3);
            setState(1255);
            expression();
            setState(1260);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1263);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 4: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::MultipleGroupingSetsContext>(
                    _localctx);
        enterOuterAlt(_localctx, 4);
        setState(1264);
        match(PrestoSqlParser::GROUPING);
        setState(1265);
        match(PrestoSqlParser::SETS);
        setState(1266);
        match(PrestoSqlParser::T__1);
        setState(1267);
        groupingSet();
        setState(1272);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1268);
          match(PrestoSqlParser::T__3);
          setState(1269);
          groupingSet();
          setState(1274);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1275);
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

std::any PrestoSqlParser::GroupingSetContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitGroupingSet(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::GroupingSetContext* PrestoSqlParser::groupingSet() {
  GroupingSetContext* _localctx =
      _tracker.createInstance<GroupingSetContext>(_ctx, getState());
  enterRule(_localctx, 62, PrestoSqlParser::RuleGroupingSet);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1292);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 163, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(1279);
        match(PrestoSqlParser::T__1);
        setState(1288);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & -6508829423092451292) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -2308677939035742217) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -18163934272260097) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -8582848577798673) != 0) ||
            _la == PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE

            || _la == PrestoSqlParser::DOUBLE_PRECISION) {
          setState(1280);
          expression();
          setState(1285);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1281);
            match(PrestoSqlParser::T__3);
            setState(1282);
            expression();
            setState(1287);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1290);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(1291);
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

std::any PrestoSqlParser::NamedQueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNamedQuery(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::NamedQueryContext* PrestoSqlParser::namedQuery() {
  NamedQueryContext* _localctx =
      _tracker.createInstance<NamedQueryContext>(_ctx, getState());
  enterRule(_localctx, 64, PrestoSqlParser::RuleNamedQuery);
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
    setState(1294);
    antlrcpp::downCast<NamedQueryContext*>(_localctx)->name = identifier();
    setState(1296);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::T__1) {
      setState(1295);
      columnAliases();
    }
    setState(1298);
    match(PrestoSqlParser::AS);
    setState(1299);
    match(PrestoSqlParser::T__1);
    setState(1300);
    query();
    setState(1301);
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

std::any PrestoSqlParser::SetQuantifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSetQuantifier(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::SetQuantifierContext* PrestoSqlParser::setQuantifier() {
  SetQuantifierContext* _localctx =
      _tracker.createInstance<SetQuantifierContext>(_ctx, getState());
  enterRule(_localctx, 66, PrestoSqlParser::RuleSetQuantifier);
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
    setState(1303);
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

PrestoSqlParser::StarModifiersContext*
PrestoSqlParser::SelectAllContext::starModifiers() {
  return getRuleContext<PrestoSqlParser::StarModifiersContext>(0);
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

std::any PrestoSqlParser::SelectAllContext::accept(
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

std::any PrestoSqlParser::SelectSingleContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSelectSingle(this);
  else
    return visitor->visitChildren(this);
}
//----------------- SelectColumnsContext
//------------------------------------------------------------------

PrestoSqlParser::QualifiedNameContext*
PrestoSqlParser::SelectColumnsContext::qualifiedName() {
  return getRuleContext<PrestoSqlParser::QualifiedNameContext>(0);
}

tree::TerminalNode* PrestoSqlParser::SelectColumnsContext::COLUMNS() {
  return getToken(PrestoSqlParser::COLUMNS, 0);
}

tree::TerminalNode* PrestoSqlParser::SelectColumnsContext::STRING() {
  return getToken(PrestoSqlParser::STRING, 0);
}

PrestoSqlParser::StarModifiersContext*
PrestoSqlParser::SelectColumnsContext::starModifiers() {
  return getRuleContext<PrestoSqlParser::StarModifiersContext>(0);
}

PrestoSqlParser::SelectColumnsContext::SelectColumnsContext(
    SelectItemContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::SelectColumnsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelectColumns(this);
}
void PrestoSqlParser::SelectColumnsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelectColumns(this);
}

std::any PrestoSqlParser::SelectColumnsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSelectColumns(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::SelectItemContext* PrestoSqlParser::selectItem() {
  SelectItemContext* _localctx =
      _tracker.createInstance<SelectItemContext>(_ctx, getState());
  enterRule(_localctx, 68, PrestoSqlParser::RuleSelectItem);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1338);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 171, _ctx)) {
      case 1: {
        _localctx = _tracker.createInstance<PrestoSqlParser::SelectAllContext>(
            _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1305);
        qualifiedName();
        setState(1306);
        match(PrestoSqlParser::T__0);
        setState(1307);
        match(PrestoSqlParser::ASTERISK);
        setState(1309);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 165, _ctx)) {
          case 1: {
            setState(1308);
            starModifiers();
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
        setState(1311);
        match(PrestoSqlParser::ASTERISK);
        setState(1313);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 166, _ctx)) {
          case 1: {
            setState(1312);
            starModifiers();
            break;
          }

          default:
            break;
        }
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SelectColumnsContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(1315);
        qualifiedName();
        setState(1316);
        match(PrestoSqlParser::T__0);
        setState(1317);
        match(PrestoSqlParser::COLUMNS);
        setState(1318);
        match(PrestoSqlParser::T__1);
        setState(1319);
        antlrcpp::downCast<SelectColumnsContext*>(_localctx)->pattern =
            match(PrestoSqlParser::STRING);
        setState(1320);
        match(PrestoSqlParser::T__2);
        setState(1322);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 167, _ctx)) {
          case 1: {
            setState(1321);
            starModifiers();
            break;
          }

          default:
            break;
        }
        break;
      }

      case 4: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SelectColumnsContext>(
                _localctx);
        enterOuterAlt(_localctx, 4);
        setState(1324);
        match(PrestoSqlParser::COLUMNS);
        setState(1325);
        match(PrestoSqlParser::T__1);
        setState(1326);
        antlrcpp::downCast<SelectColumnsContext*>(_localctx)->pattern =
            match(PrestoSqlParser::STRING);
        setState(1327);
        match(PrestoSqlParser::T__2);
        setState(1329);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 168, _ctx)) {
          case 1: {
            setState(1328);
            starModifiers();
            break;
          }

          default:
            break;
        }
        break;
      }

      case 5: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SelectSingleContext>(
                _localctx);
        enterOuterAlt(_localctx, 5);
        setState(1331);
        expression();
        setState(1336);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 170, _ctx)) {
          case 1: {
            setState(1333);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == PrestoSqlParser::AS) {
              setState(1332);
              match(PrestoSqlParser::AS);
            }
            setState(1335);
            identifier();
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

//----------------- StarModifiersContext
//------------------------------------------------------------------

PrestoSqlParser::StarModifiersContext::StarModifiersContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<PrestoSqlParser::ExcludeClauseContext*>
PrestoSqlParser::StarModifiersContext::excludeClause() {
  return getRuleContexts<PrestoSqlParser::ExcludeClauseContext>();
}

PrestoSqlParser::ExcludeClauseContext*
PrestoSqlParser::StarModifiersContext::excludeClause(size_t i) {
  return getRuleContext<PrestoSqlParser::ExcludeClauseContext>(i);
}

std::vector<PrestoSqlParser::ReplaceClauseContext*>
PrestoSqlParser::StarModifiersContext::replaceClause() {
  return getRuleContexts<PrestoSqlParser::ReplaceClauseContext>();
}

PrestoSqlParser::ReplaceClauseContext*
PrestoSqlParser::StarModifiersContext::replaceClause(size_t i) {
  return getRuleContext<PrestoSqlParser::ReplaceClauseContext>(i);
}

size_t PrestoSqlParser::StarModifiersContext::getRuleIndex() const {
  return PrestoSqlParser::RuleStarModifiers;
}

void PrestoSqlParser::StarModifiersContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterStarModifiers(this);
}

void PrestoSqlParser::StarModifiersContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitStarModifiers(this);
}

std::any PrestoSqlParser::StarModifiersContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitStarModifiers(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::StarModifiersContext* PrestoSqlParser::starModifiers() {
  StarModifiersContext* _localctx =
      _tracker.createInstance<StarModifiersContext>(_ctx, getState());
  enterRule(_localctx, 70, PrestoSqlParser::RuleStarModifiers);

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
    setState(1342);
    _errHandler->sync(this);
    alt = 1;
    do {
      switch (alt) {
        case 1: {
          setState(1342);
          _errHandler->sync(this);
          switch (_input->LA(1)) {
            case PrestoSqlParser::EXCLUDE: {
              setState(1340);
              excludeClause();
              break;
            }

            case PrestoSqlParser::REPLACE: {
              setState(1341);
              replaceClause();
              break;
            }

            default:
              throw NoViableAltException(this);
          }
          break;
        }

        default:
          throw NoViableAltException(this);
      }
      setState(1344);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 173, _ctx);
    } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExcludeClauseContext
//------------------------------------------------------------------

PrestoSqlParser::ExcludeClauseContext::ExcludeClauseContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::ExcludeClauseContext::EXCLUDE() {
  return getToken(PrestoSqlParser::EXCLUDE, 0);
}

std::vector<PrestoSqlParser::IdentifierContext*>
PrestoSqlParser::ExcludeClauseContext::identifier() {
  return getRuleContexts<PrestoSqlParser::IdentifierContext>();
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ExcludeClauseContext::identifier(size_t i) {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(i);
}

size_t PrestoSqlParser::ExcludeClauseContext::getRuleIndex() const {
  return PrestoSqlParser::RuleExcludeClause;
}

void PrestoSqlParser::ExcludeClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterExcludeClause(this);
}

void PrestoSqlParser::ExcludeClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitExcludeClause(this);
}

std::any PrestoSqlParser::ExcludeClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExcludeClause(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ExcludeClauseContext* PrestoSqlParser::excludeClause() {
  ExcludeClauseContext* _localctx =
      _tracker.createInstance<ExcludeClauseContext>(_ctx, getState());
  enterRule(_localctx, 72, PrestoSqlParser::RuleExcludeClause);
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
    setState(1346);
    match(PrestoSqlParser::EXCLUDE);
    setState(1347);
    match(PrestoSqlParser::T__1);
    setState(1348);
    identifier();
    setState(1353);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::T__3) {
      setState(1349);
      match(PrestoSqlParser::T__3);
      setState(1350);
      identifier();
      setState(1355);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(1356);
    match(PrestoSqlParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ReplaceClauseContext
//------------------------------------------------------------------

PrestoSqlParser::ReplaceClauseContext::ReplaceClauseContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::ReplaceClauseContext::REPLACE() {
  return getToken(PrestoSqlParser::REPLACE, 0);
}

std::vector<PrestoSqlParser::ReplaceItemContext*>
PrestoSqlParser::ReplaceClauseContext::replaceItem() {
  return getRuleContexts<PrestoSqlParser::ReplaceItemContext>();
}

PrestoSqlParser::ReplaceItemContext*
PrestoSqlParser::ReplaceClauseContext::replaceItem(size_t i) {
  return getRuleContext<PrestoSqlParser::ReplaceItemContext>(i);
}

size_t PrestoSqlParser::ReplaceClauseContext::getRuleIndex() const {
  return PrestoSqlParser::RuleReplaceClause;
}

void PrestoSqlParser::ReplaceClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterReplaceClause(this);
}

void PrestoSqlParser::ReplaceClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitReplaceClause(this);
}

std::any PrestoSqlParser::ReplaceClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitReplaceClause(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ReplaceClauseContext* PrestoSqlParser::replaceClause() {
  ReplaceClauseContext* _localctx =
      _tracker.createInstance<ReplaceClauseContext>(_ctx, getState());
  enterRule(_localctx, 74, PrestoSqlParser::RuleReplaceClause);
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
    setState(1358);
    match(PrestoSqlParser::REPLACE);
    setState(1359);
    match(PrestoSqlParser::T__1);
    setState(1360);
    replaceItem();
    setState(1365);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::T__3) {
      setState(1361);
      match(PrestoSqlParser::T__3);
      setState(1362);
      replaceItem();
      setState(1367);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(1368);
    match(PrestoSqlParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ReplaceItemContext
//------------------------------------------------------------------

PrestoSqlParser::ReplaceItemContext::ReplaceItemContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::ReplaceItemContext::expression() {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(0);
}

tree::TerminalNode* PrestoSqlParser::ReplaceItemContext::AS() {
  return getToken(PrestoSqlParser::AS, 0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::ReplaceItemContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

size_t PrestoSqlParser::ReplaceItemContext::getRuleIndex() const {
  return PrestoSqlParser::RuleReplaceItem;
}

void PrestoSqlParser::ReplaceItemContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterReplaceItem(this);
}

void PrestoSqlParser::ReplaceItemContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitReplaceItem(this);
}

std::any PrestoSqlParser::ReplaceItemContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitReplaceItem(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ReplaceItemContext* PrestoSqlParser::replaceItem() {
  ReplaceItemContext* _localctx =
      _tracker.createInstance<ReplaceItemContext>(_ctx, getState());
  enterRule(_localctx, 76, PrestoSqlParser::RuleReplaceItem);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1370);
    expression();
    setState(1371);
    match(PrestoSqlParser::AS);
    setState(1372);
    identifier();

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

std::any PrestoSqlParser::RelationDefaultContext::accept(
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

std::any PrestoSqlParser::JoinRelationContext::accept(
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
  size_t startState = 78;
  enterRecursionRule(_localctx, 78, PrestoSqlParser::RuleRelation, precedence);

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

    setState(1375);
    sampledRelation();
    _ctx->stop = _input->LT(-1);
    setState(1395);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 177, _ctx);
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
        setState(1377);

        if (!(precpred(_ctx, 2)))
          throw FailedPredicateException(this, "precpred(_ctx, 2)");
        setState(1391);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case PrestoSqlParser::CROSS: {
            setState(1378);
            match(PrestoSqlParser::CROSS);
            setState(1379);
            match(PrestoSqlParser::JOIN);
            setState(1380);
            antlrcpp::downCast<JoinRelationContext*>(_localctx)->right =
                sampledRelation();
            break;
          }

          case PrestoSqlParser::FULL:
          case PrestoSqlParser::INNER:
          case PrestoSqlParser::JOIN:
          case PrestoSqlParser::LEFT:
          case PrestoSqlParser::RIGHT: {
            setState(1381);
            joinType();
            setState(1382);
            match(PrestoSqlParser::JOIN);
            setState(1383);
            antlrcpp::downCast<JoinRelationContext*>(_localctx)->rightRelation =
                relation(0);
            setState(1384);
            joinCriteria();
            break;
          }

          case PrestoSqlParser::NATURAL: {
            setState(1386);
            match(PrestoSqlParser::NATURAL);
            setState(1387);
            joinType();
            setState(1388);
            match(PrestoSqlParser::JOIN);
            setState(1389);
            antlrcpp::downCast<JoinRelationContext*>(_localctx)->right =
                sampledRelation();
            break;
          }

          default:
            throw NoViableAltException(this);
        }
      }
      setState(1397);
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

std::any PrestoSqlParser::JoinTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitJoinType(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::JoinTypeContext* PrestoSqlParser::joinType() {
  JoinTypeContext* _localctx =
      _tracker.createInstance<JoinTypeContext>(_ctx, getState());
  enterRule(_localctx, 80, PrestoSqlParser::RuleJoinType);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1413);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::INNER:
      case PrestoSqlParser::JOIN: {
        enterOuterAlt(_localctx, 1);
        setState(1399);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::INNER) {
          setState(1398);
          match(PrestoSqlParser::INNER);
        }
        break;
      }

      case PrestoSqlParser::LEFT: {
        enterOuterAlt(_localctx, 2);
        setState(1401);
        match(PrestoSqlParser::LEFT);
        setState(1403);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::OUTER) {
          setState(1402);
          match(PrestoSqlParser::OUTER);
        }
        break;
      }

      case PrestoSqlParser::RIGHT: {
        enterOuterAlt(_localctx, 3);
        setState(1405);
        match(PrestoSqlParser::RIGHT);
        setState(1407);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::OUTER) {
          setState(1406);
          match(PrestoSqlParser::OUTER);
        }
        break;
      }

      case PrestoSqlParser::FULL: {
        enterOuterAlt(_localctx, 4);
        setState(1409);
        match(PrestoSqlParser::FULL);
        setState(1411);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::OUTER) {
          setState(1410);
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

std::any PrestoSqlParser::JoinCriteriaContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitJoinCriteria(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::JoinCriteriaContext* PrestoSqlParser::joinCriteria() {
  JoinCriteriaContext* _localctx =
      _tracker.createInstance<JoinCriteriaContext>(_ctx, getState());
  enterRule(_localctx, 82, PrestoSqlParser::RuleJoinCriteria);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1429);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::ON: {
        enterOuterAlt(_localctx, 1);
        setState(1415);
        match(PrestoSqlParser::ON);
        setState(1416);
        booleanExpression(0);
        break;
      }

      case PrestoSqlParser::USING: {
        enterOuterAlt(_localctx, 2);
        setState(1417);
        match(PrestoSqlParser::USING);
        setState(1418);
        match(PrestoSqlParser::T__1);
        setState(1419);
        identifier();
        setState(1424);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1420);
          match(PrestoSqlParser::T__3);
          setState(1421);
          identifier();
          setState(1426);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1427);
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

std::any PrestoSqlParser::SampledRelationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSampledRelation(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::SampledRelationContext* PrestoSqlParser::sampledRelation() {
  SampledRelationContext* _localctx =
      _tracker.createInstance<SampledRelationContext>(_ctx, getState());
  enterRule(_localctx, 84, PrestoSqlParser::RuleSampledRelation);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1431);
    aliasedRelation();
    setState(1438);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 185, _ctx)) {
      case 1: {
        setState(1432);
        match(PrestoSqlParser::TABLESAMPLE);
        setState(1433);
        sampleType();
        setState(1434);
        match(PrestoSqlParser::T__1);
        setState(1435);
        antlrcpp::downCast<SampledRelationContext*>(_localctx)->percentage =
            expression();
        setState(1436);
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

std::any PrestoSqlParser::SampleTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitSampleType(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::SampleTypeContext* PrestoSqlParser::sampleType() {
  SampleTypeContext* _localctx =
      _tracker.createInstance<SampleTypeContext>(_ctx, getState());
  enterRule(_localctx, 86, PrestoSqlParser::RuleSampleType);
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
    setState(1440);
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

std::any PrestoSqlParser::AliasedRelationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitAliasedRelation(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::AliasedRelationContext* PrestoSqlParser::aliasedRelation() {
  AliasedRelationContext* _localctx =
      _tracker.createInstance<AliasedRelationContext>(_ctx, getState());
  enterRule(_localctx, 88, PrestoSqlParser::RuleAliasedRelation);
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
    setState(1442);
    relationPrimary();
    setState(1450);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 188, _ctx)) {
      case 1: {
        setState(1444);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::AS) {
          setState(1443);
          match(PrestoSqlParser::AS);
        }
        setState(1446);
        identifier();
        setState(1448);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 187, _ctx)) {
          case 1: {
            setState(1447);
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

std::any PrestoSqlParser::ColumnAliasesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitColumnAliases(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ColumnAliasesContext* PrestoSqlParser::columnAliases() {
  ColumnAliasesContext* _localctx =
      _tracker.createInstance<ColumnAliasesContext>(_ctx, getState());
  enterRule(_localctx, 90, PrestoSqlParser::RuleColumnAliases);
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
    setState(1452);
    match(PrestoSqlParser::T__1);
    setState(1453);
    identifier();
    setState(1458);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::T__3) {
      setState(1454);
      match(PrestoSqlParser::T__3);
      setState(1455);
      identifier();
      setState(1460);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(1461);
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

std::any PrestoSqlParser::SubqueryRelationContext::accept(
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

std::any PrestoSqlParser::ParenthesizedRelationContext::accept(
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

std::any PrestoSqlParser::UnnestContext::accept(
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

std::any PrestoSqlParser::LateralContext::accept(
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

std::any PrestoSqlParser::TableNameContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTableName(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::RelationPrimaryContext* PrestoSqlParser::relationPrimary() {
  RelationPrimaryContext* _localctx =
      _tracker.createInstance<RelationPrimaryContext>(_ctx, getState());
  enterRule(_localctx, 92, PrestoSqlParser::RuleRelationPrimary);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1495);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 193, _ctx)) {
      case 1: {
        _localctx = _tracker.createInstance<PrestoSqlParser::TableNameContext>(
            _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1463);
        qualifiedName();
        setState(1465);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 190, _ctx)) {
          case 1: {
            setState(1464);
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
        setState(1467);
        match(PrestoSqlParser::T__1);
        setState(1468);
        query();
        setState(1469);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UnnestContext>(_localctx);
        enterOuterAlt(_localctx, 3);
        setState(1471);
        match(PrestoSqlParser::UNNEST);
        setState(1472);
        match(PrestoSqlParser::T__1);
        setState(1473);
        expression();
        setState(1478);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1474);
          match(PrestoSqlParser::T__3);
          setState(1475);
          expression();
          setState(1480);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1481);
        match(PrestoSqlParser::T__2);
        setState(1484);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 192, _ctx)) {
          case 1: {
            setState(1482);
            match(PrestoSqlParser::WITH);
            setState(1483);
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
        setState(1486);
        match(PrestoSqlParser::LATERAL);
        setState(1487);
        match(PrestoSqlParser::T__1);
        setState(1488);
        query();
        setState(1489);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 5: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::ParenthesizedRelationContext>(
                    _localctx);
        enterOuterAlt(_localctx, 5);
        setState(1491);
        match(PrestoSqlParser::T__1);
        setState(1492);
        relation(0);
        setState(1493);
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

std::any PrestoSqlParser::ExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExpression(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ExpressionContext* PrestoSqlParser::expression() {
  ExpressionContext* _localctx =
      _tracker.createInstance<ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 94, PrestoSqlParser::RuleExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1497);
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

std::any PrestoSqlParser::LogicalNotContext::accept(
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

std::any PrestoSqlParser::PredicatedContext::accept(
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

std::any PrestoSqlParser::LogicalBinaryContext::accept(
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
  size_t startState = 96;
  enterRecursionRule(
      _localctx, 96, PrestoSqlParser::RuleBooleanExpression, precedence);

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
    setState(1506);
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
      case PrestoSqlParser::EXCLUDE:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXECUTABLE:
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
      case PrestoSqlParser::GRAPH:
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
      case PrestoSqlParser::OPTIMIZED:
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
      case PrestoSqlParser::WINDOW:
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
      case PrestoSqlParser::TIME_WITH_TIME_ZONE:
      case PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE:
      case PrestoSqlParser::DOUBLE_PRECISION: {
        _localctx = _tracker.createInstance<PredicatedContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;

        setState(1500);
        antlrcpp::downCast<PredicatedContext*>(_localctx)
            ->valueExpressionContext = valueExpression(0);
        setState(1502);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 194, _ctx)) {
          case 1: {
            setState(1501);
            predicate(
                antlrcpp::downCast<PredicatedContext*>(_localctx)
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
        setState(1504);
        match(PrestoSqlParser::NOT);
        setState(1505);
        booleanExpression(3);
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    _ctx->stop = _input->LT(-1);
    setState(1516);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 197, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1514);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 196, _ctx)) {
          case 1: {
            auto newContext = _tracker.createInstance<LogicalBinaryContext>(
                _tracker.createInstance<BooleanExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->left = previousContext;
            pushNewRecursionContext(
                newContext, startState, RuleBooleanExpression);
            setState(1508);

            if (!(precpred(_ctx, 2)))
              throw FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(1509);
            antlrcpp::downCast<LogicalBinaryContext*>(_localctx)->op =
                match(PrestoSqlParser::AND);
            setState(1510);
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
            setState(1511);

            if (!(precpred(_ctx, 1)))
              throw FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(1512);
            antlrcpp::downCast<LogicalBinaryContext*>(_localctx)->op =
                match(PrestoSqlParser::OR);
            setState(1513);
            antlrcpp::downCast<LogicalBinaryContext*>(_localctx)->right =
                booleanExpression(2);
            break;
          }

          default:
            break;
        }
      }
      setState(1518);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 197, _ctx);
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

std::any PrestoSqlParser::ComparisonContext::accept(
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

std::any PrestoSqlParser::LikeContext::accept(tree::ParseTreeVisitor* visitor) {
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

std::any PrestoSqlParser::InSubqueryContext::accept(
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

std::any PrestoSqlParser::DistinctFromContext::accept(
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

std::any PrestoSqlParser::InListContext::accept(
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

std::any PrestoSqlParser::NullPredicateContext::accept(
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

std::any PrestoSqlParser::BetweenContext::accept(
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

std::any PrestoSqlParser::QuantifiedComparisonContext::accept(
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
  enterRule(_localctx, 98, PrestoSqlParser::RulePredicate);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1580);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 206, _ctx)) {
      case 1: {
        _localctx = _tracker.createInstance<PrestoSqlParser::ComparisonContext>(
            _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1519);
        comparisonOperator();
        setState(1520);
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
        setState(1522);
        comparisonOperator();
        setState(1523);
        comparisonQuantifier();
        setState(1524);
        match(PrestoSqlParser::T__1);
        setState(1525);
        query();
        setState(1526);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::BetweenContext>(_localctx);
        enterOuterAlt(_localctx, 3);
        setState(1529);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1528);
          match(PrestoSqlParser::NOT);
        }
        setState(1531);
        match(PrestoSqlParser::BETWEEN);
        setState(1532);
        antlrcpp::downCast<BetweenContext*>(_localctx)->lower =
            valueExpression(0);
        setState(1533);
        match(PrestoSqlParser::AND);
        setState(1534);
        antlrcpp::downCast<BetweenContext*>(_localctx)->upper =
            valueExpression(0);
        break;
      }

      case 4: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::InListContext>(_localctx);
        enterOuterAlt(_localctx, 4);
        setState(1537);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1536);
          match(PrestoSqlParser::NOT);
        }
        setState(1539);
        match(PrestoSqlParser::IN);
        setState(1540);
        match(PrestoSqlParser::T__1);
        setState(1541);
        expression();
        setState(1546);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1542);
          match(PrestoSqlParser::T__3);
          setState(1543);
          expression();
          setState(1548);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1549);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 5: {
        _localctx = _tracker.createInstance<PrestoSqlParser::InSubqueryContext>(
            _localctx);
        enterOuterAlt(_localctx, 5);
        setState(1552);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1551);
          match(PrestoSqlParser::NOT);
        }
        setState(1554);
        match(PrestoSqlParser::IN);
        setState(1555);
        match(PrestoSqlParser::T__1);
        setState(1556);
        query();
        setState(1557);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 6: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::LikeContext>(_localctx);
        enterOuterAlt(_localctx, 6);
        setState(1560);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1559);
          match(PrestoSqlParser::NOT);
        }
        setState(1562);
        match(PrestoSqlParser::LIKE);
        setState(1563);
        antlrcpp::downCast<LikeContext*>(_localctx)->pattern =
            valueExpression(0);
        setState(1566);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 203, _ctx)) {
          case 1: {
            setState(1564);
            match(PrestoSqlParser::ESCAPE);
            setState(1565);
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
        setState(1568);
        match(PrestoSqlParser::IS);
        setState(1570);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1569);
          match(PrestoSqlParser::NOT);
        }
        setState(1572);
        match(PrestoSqlParser::NULL_LITERAL);
        break;
      }

      case 8: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DistinctFromContext>(
                _localctx);
        enterOuterAlt(_localctx, 8);
        setState(1573);
        match(PrestoSqlParser::IS);
        setState(1575);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::NOT) {
          setState(1574);
          match(PrestoSqlParser::NOT);
        }
        setState(1577);
        match(PrestoSqlParser::DISTINCT);
        setState(1578);
        match(PrestoSqlParser::FROM);
        setState(1579);
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

std::any PrestoSqlParser::ValueExpressionDefaultContext::accept(
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

std::any PrestoSqlParser::ConcatenationContext::accept(
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

std::any PrestoSqlParser::ArithmeticBinaryContext::accept(
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

std::any PrestoSqlParser::ArithmeticUnaryContext::accept(
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

std::any PrestoSqlParser::AtTimeZoneContext::accept(
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
  size_t startState = 100;
  enterRecursionRule(
      _localctx, 100, PrestoSqlParser::RuleValueExpression, precedence);

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
    setState(1586);
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
      case PrestoSqlParser::EXCLUDE:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXECUTABLE:
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
      case PrestoSqlParser::GRAPH:
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
      case PrestoSqlParser::OPTIMIZED:
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
      case PrestoSqlParser::WINDOW:
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
      case PrestoSqlParser::TIME_WITH_TIME_ZONE:
      case PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE:
      case PrestoSqlParser::DOUBLE_PRECISION: {
        _localctx =
            _tracker.createInstance<ValueExpressionDefaultContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;

        setState(1583);
        primaryExpression(0);
        break;
      }

      case PrestoSqlParser::PLUS:
      case PrestoSqlParser::MINUS: {
        _localctx = _tracker.createInstance<ArithmeticUnaryContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1584);
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
        setState(1585);
        valueExpression(4);
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    _ctx->stop = _input->LT(-1);
    setState(1602);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 209, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1600);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 208, _ctx)) {
          case 1: {
            auto newContext = _tracker.createInstance<ArithmeticBinaryContext>(
                _tracker.createInstance<ValueExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->left = previousContext;
            pushNewRecursionContext(
                newContext, startState, RuleValueExpression);
            setState(1588);

            if (!(precpred(_ctx, 3)))
              throw FailedPredicateException(this, "precpred(_ctx, 3)");
            setState(1589);
            antlrcpp::downCast<ArithmeticBinaryContext*>(_localctx)->op =
                _input->LT(1);
            _la = _input->LA(1);
            if (!(((((_la - 243) & ~0x3fULL) == 0) &&
                   ((1ULL << (_la - 243)) & 7) != 0))) {
              antlrcpp::downCast<ArithmeticBinaryContext*>(_localctx)->op =
                  _errHandler->recoverInline(this);
            } else {
              _errHandler->reportMatch(this);
              consume();
            }
            setState(1590);
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
            setState(1591);

            if (!(precpred(_ctx, 2)))
              throw FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(1592);
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
            setState(1593);
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
            setState(1594);

            if (!(precpred(_ctx, 1)))
              throw FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(1595);
            match(PrestoSqlParser::CONCAT);
            setState(1596);
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
            setState(1597);

            if (!(precpred(_ctx, 5)))
              throw FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(1598);
            match(PrestoSqlParser::AT);
            setState(1599);
            timeZoneSpecifier();
            break;
          }

          default:
            break;
        }
      }
      setState(1604);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 209, _ctx);
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

std::any PrestoSqlParser::DereferenceContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitDereference(this);
  else
    return visitor->visitChildren(this);
}
//----------------- TypeConstructorContext
//------------------------------------------------------------------

PrestoSqlParser::TypeContext* PrestoSqlParser::TypeConstructorContext::type() {
  return getRuleContext<PrestoSqlParser::TypeContext>(0);
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

std::any PrestoSqlParser::TypeConstructorContext::accept(
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

std::any PrestoSqlParser::SpecialDateTimeFunctionContext::accept(
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

std::any PrestoSqlParser::SubstringContext::accept(
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

std::any PrestoSqlParser::CastContext::accept(tree::ParseTreeVisitor* visitor) {
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

std::any PrestoSqlParser::LambdaContext::accept(
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

std::any PrestoSqlParser::ParenthesizedExpressionContext::accept(
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

std::any PrestoSqlParser::ParameterContext::accept(
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

std::any PrestoSqlParser::NormalizeContext::accept(
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

std::any PrestoSqlParser::IntervalLiteralContext::accept(
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

std::any PrestoSqlParser::NumericLiteralContext::accept(
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

std::any PrestoSqlParser::BooleanLiteralContext::accept(
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

std::any PrestoSqlParser::SimpleCaseContext::accept(
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

std::any PrestoSqlParser::ColumnReferenceContext::accept(
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

std::any PrestoSqlParser::NullLiteralContext::accept(
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

std::any PrestoSqlParser::RowConstructorContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRowConstructor(this);
  else
    return visitor->visitChildren(this);
}
//----------------- NamedRowConstructorContext
//------------------------------------------------------------------

tree::TerminalNode* PrestoSqlParser::NamedRowConstructorContext::ROW() {
  return getToken(PrestoSqlParser::ROW, 0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::NamedRowConstructorContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::NamedRowConstructorContext::expression(size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

std::vector<tree::TerminalNode*>
PrestoSqlParser::NamedRowConstructorContext::AS() {
  return getTokens(PrestoSqlParser::AS);
}

tree::TerminalNode* PrestoSqlParser::NamedRowConstructorContext::AS(size_t i) {
  return getToken(PrestoSqlParser::AS, i);
}

std::vector<PrestoSqlParser::IdentifierContext*>
PrestoSqlParser::NamedRowConstructorContext::identifier() {
  return getRuleContexts<PrestoSqlParser::IdentifierContext>();
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::NamedRowConstructorContext::identifier(size_t i) {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(i);
}

PrestoSqlParser::NamedRowConstructorContext::NamedRowConstructorContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::NamedRowConstructorContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNamedRowConstructor(this);
}
void PrestoSqlParser::NamedRowConstructorContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNamedRowConstructor(this);
}

std::any PrestoSqlParser::NamedRowConstructorContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNamedRowConstructor(this);
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

std::any PrestoSqlParser::SubscriptContext::accept(
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

std::any PrestoSqlParser::SubqueryExpressionContext::accept(
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

std::any PrestoSqlParser::BinaryLiteralContext::accept(
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

std::any PrestoSqlParser::CurrentUserContext::accept(
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

std::any PrestoSqlParser::ExtractContext::accept(
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

std::any PrestoSqlParser::StringLiteralContext::accept(
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

std::any PrestoSqlParser::ArrayConstructorContext::accept(
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

std::any PrestoSqlParser::FunctionCallContext::accept(
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

std::any PrestoSqlParser::ExistsContext::accept(
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

std::any PrestoSqlParser::PositionContext::accept(
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

std::any PrestoSqlParser::SearchedCaseContext::accept(
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

std::any PrestoSqlParser::GroupingOperationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitGroupingOperation(this);
  else
    return visitor->visitChildren(this);
}
//----------------- MethodCallContext
//------------------------------------------------------------------

PrestoSqlParser::PrimaryExpressionContext*
PrestoSqlParser::MethodCallContext::primaryExpression() {
  return getRuleContext<PrestoSqlParser::PrimaryExpressionContext>(0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::MethodCallContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::MethodCallContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::MethodCallContext::expression(size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

PrestoSqlParser::MethodCallContext::MethodCallContext(
    PrimaryExpressionContext* ctx) {
  copyFrom(ctx);
}

void PrestoSqlParser::MethodCallContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterMethodCall(this);
}
void PrestoSqlParser::MethodCallContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitMethodCall(this);
}

std::any PrestoSqlParser::MethodCallContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitMethodCall(this);
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
  size_t startState = 102;
  enterRecursionRule(
      _localctx, 102, PrestoSqlParser::RulePrimaryExpression, precedence);

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
    setState(1861);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 239, _ctx)) {
      case 1: {
        _localctx = _tracker.createInstance<NullLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;

        setState(1606);
        match(PrestoSqlParser::NULL_LITERAL);
        break;
      }

      case 2: {
        _localctx = _tracker.createInstance<IntervalLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1607);
        interval();
        break;
      }

      case 3: {
        _localctx = _tracker.createInstance<TypeConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1608);
        type(0);
        setState(1609);
        string();
        break;
      }

      case 4: {
        _localctx = _tracker.createInstance<TypeConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1611);
        match(PrestoSqlParser::DOUBLE_PRECISION);
        setState(1612);
        string();
        break;
      }

      case 5: {
        _localctx = _tracker.createInstance<NumericLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1613);
        number();
        break;
      }

      case 6: {
        _localctx = _tracker.createInstance<BooleanLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1614);
        booleanValue();
        break;
      }

      case 7: {
        _localctx = _tracker.createInstance<StringLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1615);
        string();
        break;
      }

      case 8: {
        _localctx = _tracker.createInstance<BinaryLiteralContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1616);
        match(PrestoSqlParser::BINARY_LITERAL);
        break;
      }

      case 9: {
        _localctx = _tracker.createInstance<ParameterContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1617);
        match(PrestoSqlParser::T__4);
        break;
      }

      case 10: {
        _localctx = _tracker.createInstance<PositionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1618);
        match(PrestoSqlParser::POSITION);
        setState(1619);
        match(PrestoSqlParser::T__1);
        setState(1620);
        valueExpression(0);
        setState(1621);
        match(PrestoSqlParser::IN);
        setState(1622);
        valueExpression(0);
        setState(1623);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 11: {
        _localctx = _tracker.createInstance<RowConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1625);
        match(PrestoSqlParser::T__1);
        setState(1626);
        expression();
        setState(1629);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(1627);
          match(PrestoSqlParser::T__3);
          setState(1628);
          expression();
          setState(1631);
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == PrestoSqlParser::T__3);
        setState(1633);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 12: {
        _localctx = _tracker.createInstance<RowConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1635);
        match(PrestoSqlParser::ROW);
        setState(1636);
        match(PrestoSqlParser::T__1);
        setState(1637);
        expression();
        setState(1642);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1638);
          match(PrestoSqlParser::T__3);
          setState(1639);
          expression();
          setState(1644);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1645);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 13: {
        _localctx =
            _tracker.createInstance<NamedRowConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1647);
        match(PrestoSqlParser::ROW);
        setState(1648);
        match(PrestoSqlParser::T__1);
        setState(1649);
        expression();
        setState(1650);
        match(PrestoSqlParser::AS);
        setState(1651);
        identifier();
        setState(1659);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1652);
          match(PrestoSqlParser::T__3);
          setState(1653);
          expression();
          setState(1654);
          match(PrestoSqlParser::AS);
          setState(1655);
          identifier();
          setState(1661);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1662);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 14: {
        _localctx = _tracker.createInstance<FunctionCallContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1664);
        qualifiedName();
        setState(1665);
        match(PrestoSqlParser::T__1);
        setState(1666);
        match(PrestoSqlParser::ASTERISK);
        setState(1667);
        match(PrestoSqlParser::T__2);
        setState(1669);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 213, _ctx)) {
          case 1: {
            setState(1668);
            filter();
            break;
          }

          default:
            break;
        }
        setState(1672);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 214, _ctx)) {
          case 1: {
            setState(1671);
            over();
            break;
          }

          default:
            break;
        }
        break;
      }

      case 15: {
        _localctx = _tracker.createInstance<FunctionCallContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1674);
        qualifiedName();
        setState(1675);
        match(PrestoSqlParser::T__1);
        setState(1687);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & -6364714235016595420) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -2308677939035742217) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -18163934272260097) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -8582848577798673) != 0) ||
            _la == PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE

            || _la == PrestoSqlParser::DOUBLE_PRECISION) {
          setState(1677);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
              _input, 215, _ctx)) {
            case 1: {
              setState(1676);
              setQuantifier();
              break;
            }

            default:
              break;
          }
          setState(1679);
          expression();
          setState(1684);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1680);
            match(PrestoSqlParser::T__3);
            setState(1681);
            expression();
            setState(1686);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1699);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::ORDER) {
          setState(1689);
          match(PrestoSqlParser::ORDER);
          setState(1690);
          match(PrestoSqlParser::BY);
          setState(1691);
          sortItem();
          setState(1696);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1692);
            match(PrestoSqlParser::T__3);
            setState(1693);
            sortItem();
            setState(1698);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1701);
        match(PrestoSqlParser::T__2);
        setState(1703);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 220, _ctx)) {
          case 1: {
            setState(1702);
            filter();
            break;
          }

          default:
            break;
        }
        setState(1709);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 222, _ctx)) {
          case 1: {
            setState(1706);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == PrestoSqlParser::IGNORE ||
                _la == PrestoSqlParser::RESPECT) {
              setState(1705);
              nullTreatment();
            }
            setState(1708);
            over();
            break;
          }

          default:
            break;
        }
        break;
      }

      case 16: {
        _localctx = _tracker.createInstance<LambdaContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1711);
        identifier();
        setState(1712);
        match(PrestoSqlParser::T__5);
        setState(1713);
        expression();
        break;
      }

      case 17: {
        _localctx = _tracker.createInstance<LambdaContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1715);
        match(PrestoSqlParser::T__1);
        setState(1724);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & -6508956968051886080) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -2335699536833519961) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -18163934272260209) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & 8646913422763212271) != 0)) {
          setState(1716);
          identifier();
          setState(1721);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1717);
            match(PrestoSqlParser::T__3);
            setState(1718);
            identifier();
            setState(1723);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1726);
        match(PrestoSqlParser::T__2);
        setState(1727);
        match(PrestoSqlParser::T__5);
        setState(1728);
        expression();
        break;
      }

      case 18: {
        _localctx =
            _tracker.createInstance<SubqueryExpressionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1729);
        match(PrestoSqlParser::T__1);
        setState(1730);
        query();
        setState(1731);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 19: {
        _localctx = _tracker.createInstance<ExistsContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1733);
        match(PrestoSqlParser::EXISTS);
        setState(1734);
        match(PrestoSqlParser::T__1);
        setState(1735);
        query();
        setState(1736);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 20: {
        _localctx = _tracker.createInstance<SimpleCaseContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1738);
        match(PrestoSqlParser::CASE);
        setState(1739);
        valueExpression(0);
        setState(1741);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(1740);
          whenClause();
          setState(1743);
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == PrestoSqlParser::WHEN);
        setState(1747);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::ELSE) {
          setState(1745);
          match(PrestoSqlParser::ELSE);
          setState(1746);
          antlrcpp::downCast<SimpleCaseContext*>(_localctx)->elseExpression =
              expression();
        }
        setState(1749);
        match(PrestoSqlParser::END);
        break;
      }

      case 21: {
        _localctx = _tracker.createInstance<SearchedCaseContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1751);
        match(PrestoSqlParser::CASE);
        setState(1753);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(1752);
          whenClause();
          setState(1755);
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == PrestoSqlParser::WHEN);
        setState(1759);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::ELSE) {
          setState(1757);
          match(PrestoSqlParser::ELSE);
          setState(1758);
          antlrcpp::downCast<SearchedCaseContext*>(_localctx)->elseExpression =
              expression();
        }
        setState(1761);
        match(PrestoSqlParser::END);
        break;
      }

      case 22: {
        _localctx = _tracker.createInstance<CastContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1763);
        match(PrestoSqlParser::CAST);
        setState(1764);
        match(PrestoSqlParser::T__1);
        setState(1765);
        expression();
        setState(1766);
        match(PrestoSqlParser::AS);
        setState(1767);
        type(0);
        setState(1768);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 23: {
        _localctx = _tracker.createInstance<CastContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1770);
        match(PrestoSqlParser::TRY_CAST);
        setState(1771);
        match(PrestoSqlParser::T__1);
        setState(1772);
        expression();
        setState(1773);
        match(PrestoSqlParser::AS);
        setState(1774);
        type(0);
        setState(1775);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 24: {
        _localctx = _tracker.createInstance<ArrayConstructorContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1777);
        match(PrestoSqlParser::ARRAY);
        setState(1778);
        match(PrestoSqlParser::T__6);
        setState(1787);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & -6508829423092451292) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -2308677939035742217) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -18163934272260097) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -8582848577798673) != 0) ||
            _la == PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE

            || _la == PrestoSqlParser::DOUBLE_PRECISION) {
          setState(1779);
          expression();
          setState(1784);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1780);
            match(PrestoSqlParser::T__3);
            setState(1781);
            expression();
            setState(1786);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1789);
        match(PrestoSqlParser::T__7);
        break;
      }

      case 25: {
        _localctx = _tracker.createInstance<ColumnReferenceContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1790);
        identifier();
        break;
      }

      case 26: {
        _localctx =
            _tracker.createInstance<SpecialDateTimeFunctionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1791);
        antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)->name =
            match(PrestoSqlParser::CURRENT_DATE);
        break;
      }

      case 27: {
        _localctx =
            _tracker.createInstance<SpecialDateTimeFunctionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1792);
        antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)->name =
            match(PrestoSqlParser::CURRENT_TIME);
        setState(1796);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 231, _ctx)) {
          case 1: {
            setState(1793);
            match(PrestoSqlParser::T__1);
            setState(1794);
            antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)
                ->precision = match(PrestoSqlParser::INTEGER_VALUE);
            setState(1795);
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
        setState(1798);
        antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)->name =
            match(PrestoSqlParser::CURRENT_TIMESTAMP);
        setState(1802);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 232, _ctx)) {
          case 1: {
            setState(1799);
            match(PrestoSqlParser::T__1);
            setState(1800);
            antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)
                ->precision = match(PrestoSqlParser::INTEGER_VALUE);
            setState(1801);
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
        setState(1804);
        antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)->name =
            match(PrestoSqlParser::LOCALTIME);
        setState(1808);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 233, _ctx)) {
          case 1: {
            setState(1805);
            match(PrestoSqlParser::T__1);
            setState(1806);
            antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)
                ->precision = match(PrestoSqlParser::INTEGER_VALUE);
            setState(1807);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 30: {
        _localctx =
            _tracker.createInstance<SpecialDateTimeFunctionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1810);
        antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)->name =
            match(PrestoSqlParser::LOCALTIMESTAMP);
        setState(1814);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 234, _ctx)) {
          case 1: {
            setState(1811);
            match(PrestoSqlParser::T__1);
            setState(1812);
            antlrcpp::downCast<SpecialDateTimeFunctionContext*>(_localctx)
                ->precision = match(PrestoSqlParser::INTEGER_VALUE);
            setState(1813);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 31: {
        _localctx = _tracker.createInstance<CurrentUserContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1816);
        antlrcpp::downCast<CurrentUserContext*>(_localctx)->name =
            match(PrestoSqlParser::CURRENT_USER);
        break;
      }

      case 32: {
        _localctx = _tracker.createInstance<SubstringContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1817);
        match(PrestoSqlParser::SUBSTRING);
        setState(1818);
        match(PrestoSqlParser::T__1);
        setState(1819);
        valueExpression(0);
        setState(1820);
        match(PrestoSqlParser::FROM);
        setState(1821);
        valueExpression(0);
        setState(1824);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::FOR) {
          setState(1822);
          match(PrestoSqlParser::FOR);
          setState(1823);
          valueExpression(0);
        }
        setState(1826);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 33: {
        _localctx = _tracker.createInstance<NormalizeContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1828);
        match(PrestoSqlParser::NORMALIZE);
        setState(1829);
        match(PrestoSqlParser::T__1);
        setState(1830);
        valueExpression(0);
        setState(1833);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::T__3) {
          setState(1831);
          match(PrestoSqlParser::T__3);
          setState(1832);
          normalForm();
        }
        setState(1835);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 34: {
        _localctx = _tracker.createInstance<ExtractContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1837);
        match(PrestoSqlParser::EXTRACT);
        setState(1838);
        match(PrestoSqlParser::T__1);
        setState(1839);
        identifier();
        setState(1840);
        match(PrestoSqlParser::FROM);
        setState(1841);
        valueExpression(0);
        setState(1842);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 35: {
        _localctx =
            _tracker.createInstance<ParenthesizedExpressionContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1844);
        match(PrestoSqlParser::T__1);
        setState(1845);
        expression();
        setState(1846);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 36: {
        _localctx =
            _tracker.createInstance<GroupingOperationContext>(_localctx);
        _ctx = _localctx;
        previousContext = _localctx;
        setState(1848);
        match(PrestoSqlParser::GROUPING);
        setState(1849);
        match(PrestoSqlParser::T__1);
        setState(1858);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & -6508956968051886080) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -2335699536833519961) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -18163934272260209) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & 8646913422763212271) != 0)) {
          setState(1850);
          qualifiedName();
          setState(1855);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == PrestoSqlParser::T__3) {
            setState(1851);
            match(PrestoSqlParser::T__3);
            setState(1852);
            qualifiedName();
            setState(1857);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(1860);
        match(PrestoSqlParser::T__2);
        break;
      }

      default:
        break;
    }
    _ctx->stop = _input->LT(-1);
    setState(1889);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 243, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        setState(1887);
        _errHandler->sync(this);
        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 242, _ctx)) {
          case 1: {
            auto newContext = _tracker.createInstance<SubscriptContext>(
                _tracker.createInstance<PrimaryExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->value = previousContext;
            pushNewRecursionContext(
                newContext, startState, RulePrimaryExpression);
            setState(1863);

            if (!(precpred(_ctx, 15)))
              throw FailedPredicateException(this, "precpred(_ctx, 15)");
            setState(1864);
            match(PrestoSqlParser::T__6);
            setState(1865);
            antlrcpp::downCast<SubscriptContext*>(_localctx)->index =
                valueExpression(0);
            setState(1866);
            match(PrestoSqlParser::T__7);
            break;
          }

          case 2: {
            auto newContext = _tracker.createInstance<MethodCallContext>(
                _tracker.createInstance<PrimaryExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->base = previousContext;
            pushNewRecursionContext(
                newContext, startState, RulePrimaryExpression);
            setState(1868);

            if (!(precpred(_ctx, 13)))
              throw FailedPredicateException(this, "precpred(_ctx, 13)");
            setState(1869);
            match(PrestoSqlParser::T__0);
            setState(1870);
            antlrcpp::downCast<MethodCallContext*>(_localctx)->functionName =
                identifier();
            setState(1871);
            match(PrestoSqlParser::T__1);
            setState(1880);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if ((((_la & ~0x3fULL) == 0) &&
                 ((1ULL << _la) & -6508829423092451292) != 0) ||
                ((((_la - 66) & ~0x3fULL) == 0) &&
                 ((1ULL << (_la - 66)) & -2308677939035742217) != 0) ||
                ((((_la - 130) & ~0x3fULL) == 0) &&
                 ((1ULL << (_la - 130)) & -18163934272260097) != 0) ||
                ((((_la - 194) & ~0x3fULL) == 0) &&
                 ((1ULL << (_la - 194)) & -8582848577798673) != 0) ||
                _la == PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE

                || _la == PrestoSqlParser::DOUBLE_PRECISION) {
              setState(1872);
              antlrcpp::downCast<MethodCallContext*>(_localctx)
                  ->expressionContext = expression();
              antlrcpp::downCast<MethodCallContext*>(_localctx)
                  ->arguments.push_back(
                      antlrcpp::downCast<MethodCallContext*>(_localctx)
                          ->expressionContext);
              setState(1877);
              _errHandler->sync(this);
              _la = _input->LA(1);
              while (_la == PrestoSqlParser::T__3) {
                setState(1873);
                match(PrestoSqlParser::T__3);
                setState(1874);
                antlrcpp::downCast<MethodCallContext*>(_localctx)
                    ->expressionContext = expression();
                antlrcpp::downCast<MethodCallContext*>(_localctx)
                    ->arguments.push_back(
                        antlrcpp::downCast<MethodCallContext*>(_localctx)
                            ->expressionContext);
                setState(1879);
                _errHandler->sync(this);
                _la = _input->LA(1);
              }
            }
            setState(1882);
            match(PrestoSqlParser::T__2);
            break;
          }

          case 3: {
            auto newContext = _tracker.createInstance<DereferenceContext>(
                _tracker.createInstance<PrimaryExpressionContext>(
                    parentContext, parentState));
            _localctx = newContext;
            newContext->base = previousContext;
            pushNewRecursionContext(
                newContext, startState, RulePrimaryExpression);
            setState(1884);

            if (!(precpred(_ctx, 12)))
              throw FailedPredicateException(this, "precpred(_ctx, 12)");
            setState(1885);
            match(PrestoSqlParser::T__0);
            setState(1886);
            antlrcpp::downCast<DereferenceContext*>(_localctx)->fieldName =
                identifier();
            break;
          }

          default:
            break;
        }
      }
      setState(1891);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 243, _ctx);
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

std::any PrestoSqlParser::UnicodeStringLiteralContext::accept(
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

std::any PrestoSqlParser::BasicStringLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBasicStringLiteral(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::StringContext* PrestoSqlParser::string() {
  StringContext* _localctx =
      _tracker.createInstance<StringContext>(_ctx, getState());
  enterRule(_localctx, 104, PrestoSqlParser::RuleString);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1898);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::STRING: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::BasicStringLiteralContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1892);
        match(PrestoSqlParser::STRING);
        break;
      }

      case PrestoSqlParser::UNICODE_STRING: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::UnicodeStringLiteralContext>(
                    _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1893);
        match(PrestoSqlParser::UNICODE_STRING);
        setState(1896);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 244, _ctx)) {
          case 1: {
            setState(1894);
            match(PrestoSqlParser::UESCAPE);
            setState(1895);
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

std::any PrestoSqlParser::NullTreatmentContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNullTreatment(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::NullTreatmentContext* PrestoSqlParser::nullTreatment() {
  NullTreatmentContext* _localctx =
      _tracker.createInstance<NullTreatmentContext>(_ctx, getState());
  enterRule(_localctx, 106, PrestoSqlParser::RuleNullTreatment);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1904);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::IGNORE: {
        enterOuterAlt(_localctx, 1);
        setState(1900);
        match(PrestoSqlParser::IGNORE);
        setState(1901);
        match(PrestoSqlParser::NULLS);
        break;
      }

      case PrestoSqlParser::RESPECT: {
        enterOuterAlt(_localctx, 2);
        setState(1902);
        match(PrestoSqlParser::RESPECT);
        setState(1903);
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

std::any PrestoSqlParser::TimeZoneIntervalContext::accept(
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

std::any PrestoSqlParser::TimeZoneStringContext::accept(
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
  enterRule(_localctx, 108, PrestoSqlParser::RuleTimeZoneSpecifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1912);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 247, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TimeZoneIntervalContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(1906);
        match(PrestoSqlParser::TIME);
        setState(1907);
        match(PrestoSqlParser::ZONE);
        setState(1908);
        interval();
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TimeZoneStringContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(1909);
        match(PrestoSqlParser::TIME);
        setState(1910);
        match(PrestoSqlParser::ZONE);
        setState(1911);
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

std::any PrestoSqlParser::ComparisonOperatorContext::accept(
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
  enterRule(_localctx, 110, PrestoSqlParser::RuleComparisonOperator);
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
    setState(1914);
    _la = _input->LA(1);
    if (!(((((_la - 235) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 235)) & 63) != 0))) {
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

std::any PrestoSqlParser::ComparisonQuantifierContext::accept(
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
  enterRule(_localctx, 112, PrestoSqlParser::RuleComparisonQuantifier);
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
    setState(1916);
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

std::any PrestoSqlParser::BooleanValueContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBooleanValue(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::BooleanValueContext* PrestoSqlParser::booleanValue() {
  BooleanValueContext* _localctx =
      _tracker.createInstance<BooleanValueContext>(_ctx, getState());
  enterRule(_localctx, 114, PrestoSqlParser::RuleBooleanValue);
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
    setState(1918);
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

std::any PrestoSqlParser::IntervalContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitInterval(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::IntervalContext* PrestoSqlParser::interval() {
  IntervalContext* _localctx =
      _tracker.createInstance<IntervalContext>(_ctx, getState());
  enterRule(_localctx, 116, PrestoSqlParser::RuleInterval);
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
    setState(1920);
    match(PrestoSqlParser::INTERVAL);
    setState(1922);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::PLUS

        || _la == PrestoSqlParser::MINUS) {
      setState(1921);
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
    setState(1924);
    string();
    setState(1925);
    antlrcpp::downCast<IntervalContext*>(_localctx)->from = intervalField();
    setState(1928);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 249, _ctx)) {
      case 1: {
        setState(1926);
        match(PrestoSqlParser::TO);
        setState(1927);
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

std::any PrestoSqlParser::IntervalFieldContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitIntervalField(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::IntervalFieldContext* PrestoSqlParser::intervalField() {
  IntervalFieldContext* _localctx =
      _tracker.createInstance<IntervalFieldContext>(_ctx, getState());
  enterRule(_localctx, 118, PrestoSqlParser::RuleIntervalField);
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
    setState(1930);
    _la = _input->LA(1);
    if (!(_la == PrestoSqlParser::DAY ||
          ((((_la - 94) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 94)) & 3221225473) != 0) ||
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

std::any PrestoSqlParser::NormalFormContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNormalForm(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::NormalFormContext* PrestoSqlParser::normalForm() {
  NormalFormContext* _localctx =
      _tracker.createInstance<NormalFormContext>(_ctx, getState());
  enterRule(_localctx, 120, PrestoSqlParser::RuleNormalForm);
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
    setState(1932);
    _la = _input->LA(1);
    if (!(((((_la - 128) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 128)) & 15) != 0))) {
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

std::any PrestoSqlParser::TypesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTypes(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::TypesContext* PrestoSqlParser::types() {
  TypesContext* _localctx =
      _tracker.createInstance<TypesContext>(_ctx, getState());
  enterRule(_localctx, 122, PrestoSqlParser::RuleTypes);
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
    setState(1934);
    match(PrestoSqlParser::T__1);
    setState(1943);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 11) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 11)) & 6022638099777167063) != 0) ||
        ((((_la - 75) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 75)) & -4039787179281842385) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) & -576496228737548997) != 0) ||
        ((((_la - 204) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 204)) & 71494646172604143) != 0)) {
      setState(1935);
      type(0);
      setState(1940);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PrestoSqlParser::T__3) {
        setState(1936);
        match(PrestoSqlParser::T__3);
        setState(1937);
        type(0);
        setState(1942);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1945);
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

std::any PrestoSqlParser::TypeContext::accept(tree::ParseTreeVisitor* visitor) {
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
  size_t startState = 124;
  enterRecursionRule(_localctx, 124, PrestoSqlParser::RuleType, precedence);

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
    setState(1994);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 255, _ctx)) {
      case 1: {
        setState(1948);
        match(PrestoSqlParser::ARRAY);
        setState(1949);
        match(PrestoSqlParser::LT);
        setState(1950);
        type(0);
        setState(1951);
        match(PrestoSqlParser::GT);
        break;
      }

      case 2: {
        setState(1953);
        match(PrestoSqlParser::MAP);
        setState(1954);
        match(PrestoSqlParser::LT);
        setState(1955);
        type(0);
        setState(1956);
        match(PrestoSqlParser::T__3);
        setState(1957);
        type(0);
        setState(1958);
        match(PrestoSqlParser::GT);
        break;
      }

      case 3: {
        setState(1960);
        match(PrestoSqlParser::ROW);
        setState(1961);
        match(PrestoSqlParser::T__1);
        setState(1962);
        identifier();
        setState(1963);
        type(0);
        setState(1970);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == PrestoSqlParser::T__3) {
          setState(1964);
          match(PrestoSqlParser::T__3);
          setState(1965);
          identifier();
          setState(1966);
          type(0);
          setState(1972);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(1973);
        match(PrestoSqlParser::T__2);
        break;
      }

      case 4: {
        setState(1975);
        baseType();
        setState(1987);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 254, _ctx)) {
          case 1: {
            setState(1976);
            match(PrestoSqlParser::T__1);
            setState(1977);
            typeParameter();
            setState(1982);
            _errHandler->sync(this);
            _la = _input->LA(1);
            while (_la == PrestoSqlParser::T__3) {
              setState(1978);
              match(PrestoSqlParser::T__3);
              setState(1979);
              typeParameter();
              setState(1984);
              _errHandler->sync(this);
              _la = _input->LA(1);
            }
            setState(1985);
            match(PrestoSqlParser::T__2);
            break;
          }

          default:
            break;
        }
        break;
      }

      case 5: {
        setState(1989);
        match(PrestoSqlParser::INTERVAL);
        setState(1990);
        antlrcpp::downCast<TypeContext*>(_localctx)->from = intervalField();
        setState(1991);
        match(PrestoSqlParser::TO);
        setState(1992);
        antlrcpp::downCast<TypeContext*>(_localctx)->to = intervalField();
        break;
      }

      default:
        break;
    }
    _ctx->stop = _input->LT(-1);
    setState(2000);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 256, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        if (!_parseListeners.empty())
          triggerExitRuleEvent();
        previousContext = _localctx;
        _localctx =
            _tracker.createInstance<TypeContext>(parentContext, parentState);
        pushNewRecursionContext(_localctx, startState, RuleType);
        setState(1996);

        if (!(precpred(_ctx, 6)))
          throw FailedPredicateException(this, "precpred(_ctx, 6)");
        setState(1997);
        match(PrestoSqlParser::ARRAY);
      }
      setState(2002);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 256, _ctx);
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

std::any PrestoSqlParser::TypeParameterContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitTypeParameter(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::TypeParameterContext* PrestoSqlParser::typeParameter() {
  TypeParameterContext* _localctx =
      _tracker.createInstance<TypeParameterContext>(_ctx, getState());
  enterRule(_localctx, 126, PrestoSqlParser::RuleTypeParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2005);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::INTEGER_VALUE: {
        enterOuterAlt(_localctx, 1);
        setState(2003);
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
      case PrestoSqlParser::EXCLUDE:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXECUTABLE:
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
      case PrestoSqlParser::GRAPH:
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
      case PrestoSqlParser::OPTIMIZED:
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
      case PrestoSqlParser::WINDOW:
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
        setState(2004);
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

std::any PrestoSqlParser::BaseTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitBaseType(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::BaseTypeContext* PrestoSqlParser::baseType() {
  BaseTypeContext* _localctx =
      _tracker.createInstance<BaseTypeContext>(_ctx, getState());
  enterRule(_localctx, 128, PrestoSqlParser::RuleBaseType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2011);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::TIME_WITH_TIME_ZONE: {
        enterOuterAlt(_localctx, 1);
        setState(2007);
        match(PrestoSqlParser::TIME_WITH_TIME_ZONE);
        break;
      }

      case PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE: {
        enterOuterAlt(_localctx, 2);
        setState(2008);
        match(PrestoSqlParser::TIMESTAMP_WITH_TIME_ZONE);
        break;
      }

      case PrestoSqlParser::DOUBLE_PRECISION: {
        enterOuterAlt(_localctx, 3);
        setState(2009);
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
      case PrestoSqlParser::EXCLUDE:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXECUTABLE:
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
      case PrestoSqlParser::GRAPH:
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
      case PrestoSqlParser::OPTIMIZED:
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
      case PrestoSqlParser::WINDOW:
      case PrestoSqlParser::WORK:
      case PrestoSqlParser::WRITE:
      case PrestoSqlParser::YEAR:
      case PrestoSqlParser::ZONE:
      case PrestoSqlParser::IDENTIFIER:
      case PrestoSqlParser::DIGIT_IDENTIFIER:
      case PrestoSqlParser::QUOTED_IDENTIFIER:
      case PrestoSqlParser::BACKQUOTED_IDENTIFIER: {
        enterOuterAlt(_localctx, 4);
        setState(2010);
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

std::any PrestoSqlParser::WhenClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitWhenClause(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::WhenClauseContext* PrestoSqlParser::whenClause() {
  WhenClauseContext* _localctx =
      _tracker.createInstance<WhenClauseContext>(_ctx, getState());
  enterRule(_localctx, 130, PrestoSqlParser::RuleWhenClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2013);
    match(PrestoSqlParser::WHEN);
    setState(2014);
    antlrcpp::downCast<WhenClauseContext*>(_localctx)->condition = expression();
    setState(2015);
    match(PrestoSqlParser::THEN);
    setState(2016);
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

std::any PrestoSqlParser::FilterContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitFilter(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::FilterContext* PrestoSqlParser::filter() {
  FilterContext* _localctx =
      _tracker.createInstance<FilterContext>(_ctx, getState());
  enterRule(_localctx, 132, PrestoSqlParser::RuleFilter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2018);
    match(PrestoSqlParser::FILTER);
    setState(2019);
    match(PrestoSqlParser::T__1);
    setState(2020);
    match(PrestoSqlParser::WHERE);
    setState(2021);
    booleanExpression(0);
    setState(2022);
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

PrestoSqlParser::IdentifierContext* PrestoSqlParser::OverContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

PrestoSqlParser::WindowSpecificationContext*
PrestoSqlParser::OverContext::windowSpecification() {
  return getRuleContext<PrestoSqlParser::WindowSpecificationContext>(0);
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

std::any PrestoSqlParser::OverContext::accept(tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitOver(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::OverContext* PrestoSqlParser::over() {
  OverContext* _localctx =
      _tracker.createInstance<OverContext>(_ctx, getState());
  enterRule(_localctx, 134, PrestoSqlParser::RuleOver);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2031);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 259, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(2024);
        match(PrestoSqlParser::OVER);
        setState(2025);
        identifier();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(2026);
        match(PrestoSqlParser::OVER);
        setState(2027);
        match(PrestoSqlParser::T__1);
        setState(2028);
        windowSpecification();
        setState(2029);
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

//----------------- WindowSpecificationContext
//------------------------------------------------------------------

PrestoSqlParser::WindowSpecificationContext::WindowSpecificationContext(
    ParserRuleContext* parent,
    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* PrestoSqlParser::WindowSpecificationContext::PARTITION() {
  return getToken(PrestoSqlParser::PARTITION, 0);
}

std::vector<tree::TerminalNode*>
PrestoSqlParser::WindowSpecificationContext::BY() {
  return getTokens(PrestoSqlParser::BY);
}

tree::TerminalNode* PrestoSqlParser::WindowSpecificationContext::BY(size_t i) {
  return getToken(PrestoSqlParser::BY, i);
}

tree::TerminalNode* PrestoSqlParser::WindowSpecificationContext::ORDER() {
  return getToken(PrestoSqlParser::ORDER, 0);
}

std::vector<PrestoSqlParser::SortItemContext*>
PrestoSqlParser::WindowSpecificationContext::sortItem() {
  return getRuleContexts<PrestoSqlParser::SortItemContext>();
}

PrestoSqlParser::SortItemContext*
PrestoSqlParser::WindowSpecificationContext::sortItem(size_t i) {
  return getRuleContext<PrestoSqlParser::SortItemContext>(i);
}

PrestoSqlParser::WindowFrameContext*
PrestoSqlParser::WindowSpecificationContext::windowFrame() {
  return getRuleContext<PrestoSqlParser::WindowFrameContext>(0);
}

PrestoSqlParser::IdentifierContext*
PrestoSqlParser::WindowSpecificationContext::identifier() {
  return getRuleContext<PrestoSqlParser::IdentifierContext>(0);
}

std::vector<PrestoSqlParser::ExpressionContext*>
PrestoSqlParser::WindowSpecificationContext::expression() {
  return getRuleContexts<PrestoSqlParser::ExpressionContext>();
}

PrestoSqlParser::ExpressionContext*
PrestoSqlParser::WindowSpecificationContext::expression(size_t i) {
  return getRuleContext<PrestoSqlParser::ExpressionContext>(i);
}

size_t PrestoSqlParser::WindowSpecificationContext::getRuleIndex() const {
  return PrestoSqlParser::RuleWindowSpecification;
}

void PrestoSqlParser::WindowSpecificationContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterWindowSpecification(this);
}

void PrestoSqlParser::WindowSpecificationContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<PrestoSqlListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitWindowSpecification(this);
}

std::any PrestoSqlParser::WindowSpecificationContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitWindowSpecification(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::WindowSpecificationContext*
PrestoSqlParser::windowSpecification() {
  WindowSpecificationContext* _localctx =
      _tracker.createInstance<WindowSpecificationContext>(_ctx, getState());
  enterRule(_localctx, 136, PrestoSqlParser::RuleWindowSpecification);
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
    setState(2034);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 260, _ctx)) {
      case 1: {
        setState(2033);
        antlrcpp::downCast<WindowSpecificationContext*>(_localctx)
            ->existingWindowName = identifier();
        break;
      }

      default:
        break;
    }
    setState(2046);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::PARTITION) {
      setState(2036);
      match(PrestoSqlParser::PARTITION);
      setState(2037);
      match(PrestoSqlParser::BY);
      setState(2038);
      antlrcpp::downCast<WindowSpecificationContext*>(_localctx)
          ->expressionContext = expression();
      antlrcpp::downCast<WindowSpecificationContext*>(_localctx)
          ->partition.push_back(
              antlrcpp::downCast<WindowSpecificationContext*>(_localctx)
                  ->expressionContext);
      setState(2043);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PrestoSqlParser::T__3) {
        setState(2039);
        match(PrestoSqlParser::T__3);
        setState(2040);
        antlrcpp::downCast<WindowSpecificationContext*>(_localctx)
            ->expressionContext = expression();
        antlrcpp::downCast<WindowSpecificationContext*>(_localctx)
            ->partition.push_back(
                antlrcpp::downCast<WindowSpecificationContext*>(_localctx)
                    ->expressionContext);
        setState(2045);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(2058);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::ORDER) {
      setState(2048);
      match(PrestoSqlParser::ORDER);
      setState(2049);
      match(PrestoSqlParser::BY);
      setState(2050);
      sortItem();
      setState(2055);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == PrestoSqlParser::T__3) {
        setState(2051);
        match(PrestoSqlParser::T__3);
        setState(2052);
        sortItem();
        setState(2057);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(2061);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == PrestoSqlParser::GROUPS || _la == PrestoSqlParser::RANGE

        || _la == PrestoSqlParser::ROWS) {
      setState(2060);
      windowFrame();
    }

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

std::any PrestoSqlParser::WindowFrameContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitWindowFrame(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::WindowFrameContext* PrestoSqlParser::windowFrame() {
  WindowFrameContext* _localctx =
      _tracker.createInstance<WindowFrameContext>(_ctx, getState());
  enterRule(_localctx, 138, PrestoSqlParser::RuleWindowFrame);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2087);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 266, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(2063);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::RANGE);
        setState(2064);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(2065);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::ROWS);
        setState(2066);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(2067);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::GROUPS);
        setState(2068);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        break;
      }

      case 4: {
        enterOuterAlt(_localctx, 4);
        setState(2069);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::RANGE);
        setState(2070);
        match(PrestoSqlParser::BETWEEN);
        setState(2071);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        setState(2072);
        match(PrestoSqlParser::AND);
        setState(2073);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->end = frameBound();
        break;
      }

      case 5: {
        enterOuterAlt(_localctx, 5);
        setState(2075);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::ROWS);
        setState(2076);
        match(PrestoSqlParser::BETWEEN);
        setState(2077);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        setState(2078);
        match(PrestoSqlParser::AND);
        setState(2079);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->end = frameBound();
        break;
      }

      case 6: {
        enterOuterAlt(_localctx, 6);
        setState(2081);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->frameType =
            match(PrestoSqlParser::GROUPS);
        setState(2082);
        match(PrestoSqlParser::BETWEEN);
        setState(2083);
        antlrcpp::downCast<WindowFrameContext*>(_localctx)->start =
            frameBound();
        setState(2084);
        match(PrestoSqlParser::AND);
        setState(2085);
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

std::any PrestoSqlParser::BoundedFrameContext::accept(
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

std::any PrestoSqlParser::UnboundedFrameContext::accept(
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

std::any PrestoSqlParser::CurrentRowBoundContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCurrentRowBound(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::FrameBoundContext* PrestoSqlParser::frameBound() {
  FrameBoundContext* _localctx =
      _tracker.createInstance<FrameBoundContext>(_ctx, getState());
  enterRule(_localctx, 140, PrestoSqlParser::RuleFrameBound);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2098);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 267, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UnboundedFrameContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2089);
        match(PrestoSqlParser::UNBOUNDED);
        setState(2090);
        antlrcpp::downCast<UnboundedFrameContext*>(_localctx)->boundType =
            match(PrestoSqlParser::PRECEDING);
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UnboundedFrameContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2091);
        match(PrestoSqlParser::UNBOUNDED);
        setState(2092);
        antlrcpp::downCast<UnboundedFrameContext*>(_localctx)->boundType =
            match(PrestoSqlParser::FOLLOWING);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CurrentRowBoundContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(2093);
        match(PrestoSqlParser::CURRENT);
        setState(2094);
        match(PrestoSqlParser::ROW);
        break;
      }

      case 4: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::BoundedFrameContext>(
                _localctx);
        enterOuterAlt(_localctx, 4);
        setState(2095);
        expression();
        setState(2096);
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

std::any PrestoSqlParser::UpdateAssignmentContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUpdateAssignment(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::UpdateAssignmentContext* PrestoSqlParser::updateAssignment() {
  UpdateAssignmentContext* _localctx =
      _tracker.createInstance<UpdateAssignmentContext>(_ctx, getState());
  enterRule(_localctx, 142, PrestoSqlParser::RuleUpdateAssignment);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2100);
    identifier();
    setState(2101);
    match(PrestoSqlParser::EQ);
    setState(2102);
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

std::any PrestoSqlParser::ExplainFormatContext::accept(
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

tree::TerminalNode* PrestoSqlParser::ExplainTypeContext::GRAPH() {
  return getToken(PrestoSqlParser::GRAPH, 0);
}

tree::TerminalNode* PrestoSqlParser::ExplainTypeContext::OPTIMIZED() {
  return getToken(PrestoSqlParser::OPTIMIZED, 0);
}

tree::TerminalNode* PrestoSqlParser::ExplainTypeContext::EXECUTABLE() {
  return getToken(PrestoSqlParser::EXECUTABLE, 0);
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

tree::TerminalNode* PrestoSqlParser::ExplainTypeContext::WITH() {
  return getToken(PrestoSqlParser::WITH, 0);
}

PrestoSqlParser::PropertiesContext*
PrestoSqlParser::ExplainTypeContext::properties() {
  return getRuleContext<PrestoSqlParser::PropertiesContext>(0);
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

std::any PrestoSqlParser::ExplainTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitExplainType(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::ExplainOptionContext* PrestoSqlParser::explainOption() {
  ExplainOptionContext* _localctx =
      _tracker.createInstance<ExplainOptionContext>(_ctx, getState());
  enterRule(_localctx, 144, PrestoSqlParser::RuleExplainOption);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2112);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::FORMAT: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ExplainFormatContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2104);
        match(PrestoSqlParser::FORMAT);
        setState(2105);
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
        setState(2106);
        match(PrestoSqlParser::TYPE);
        setState(2107);
        antlrcpp::downCast<ExplainTypeContext*>(_localctx)->value =
            _input->LT(1);
        _la = _input->LA(1);
        if (!(((((_la - 58) & ~0x3fULL) == 0) &&
               ((1ULL << (_la - 58)) & -9223090560804322303) != 0) ||
              _la == PrestoSqlParser::OPTIMIZED ||
              _la == PrestoSqlParser::VALIDATE)) {
          antlrcpp::downCast<ExplainTypeContext*>(_localctx)->value =
              _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(2110);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == PrestoSqlParser::WITH) {
          setState(2108);
          match(PrestoSqlParser::WITH);
          setState(2109);
          properties();
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

std::any PrestoSqlParser::TransactionAccessModeContext::accept(
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

std::any PrestoSqlParser::IsolationLevelContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitIsolationLevel(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::TransactionModeContext* PrestoSqlParser::transactionMode() {
  TransactionModeContext* _localctx =
      _tracker.createInstance<TransactionModeContext>(_ctx, getState());
  enterRule(_localctx, 146, PrestoSqlParser::RuleTransactionMode);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2119);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::ISOLATION: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::IsolationLevelContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2114);
        match(PrestoSqlParser::ISOLATION);
        setState(2115);
        match(PrestoSqlParser::LEVEL);
        setState(2116);
        levelOfIsolation();
        break;
      }

      case PrestoSqlParser::READ: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::TransactionAccessModeContext>(
                    _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2117);
        match(PrestoSqlParser::READ);
        setState(2118);
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

std::any PrestoSqlParser::ReadUncommittedContext::accept(
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

std::any PrestoSqlParser::SerializableContext::accept(
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

std::any PrestoSqlParser::ReadCommittedContext::accept(
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

std::any PrestoSqlParser::RepeatableReadContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRepeatableRead(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::LevelOfIsolationContext* PrestoSqlParser::levelOfIsolation() {
  LevelOfIsolationContext* _localctx =
      _tracker.createInstance<LevelOfIsolationContext>(_ctx, getState());
  enterRule(_localctx, 148, PrestoSqlParser::RuleLevelOfIsolation);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2128);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 271, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ReadUncommittedContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2121);
        match(PrestoSqlParser::READ);
        setState(2122);
        match(PrestoSqlParser::UNCOMMITTED);
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::ReadCommittedContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2123);
        match(PrestoSqlParser::READ);
        setState(2124);
        match(PrestoSqlParser::COMMITTED);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RepeatableReadContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(2125);
        match(PrestoSqlParser::REPEATABLE);
        setState(2126);
        match(PrestoSqlParser::READ);
        break;
      }

      case 4: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SerializableContext>(
                _localctx);
        enterOuterAlt(_localctx, 4);
        setState(2127);
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

std::any PrestoSqlParser::PositionalArgumentContext::accept(
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

std::any PrestoSqlParser::NamedArgumentContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNamedArgument(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::CallArgumentContext* PrestoSqlParser::callArgument() {
  CallArgumentContext* _localctx =
      _tracker.createInstance<CallArgumentContext>(_ctx, getState());
  enterRule(_localctx, 150, PrestoSqlParser::RuleCallArgument);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2135);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 272, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::PositionalArgumentContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2130);
        expression();
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::NamedArgumentContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2131);
        identifier();
        setState(2132);
        match(PrestoSqlParser::T__8);
        setState(2133);
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

std::any PrestoSqlParser::PrivilegeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitPrivilege(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::PrivilegeContext* PrestoSqlParser::privilege() {
  PrivilegeContext* _localctx =
      _tracker.createInstance<PrivilegeContext>(_ctx, getState());
  enterRule(_localctx, 152, PrestoSqlParser::RulePrivilege);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2141);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::SELECT: {
        enterOuterAlt(_localctx, 1);
        setState(2137);
        match(PrestoSqlParser::SELECT);
        break;
      }

      case PrestoSqlParser::DELETE: {
        enterOuterAlt(_localctx, 2);
        setState(2138);
        match(PrestoSqlParser::DELETE);
        break;
      }

      case PrestoSqlParser::INSERT: {
        enterOuterAlt(_localctx, 3);
        setState(2139);
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
      case PrestoSqlParser::EXCLUDE:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXECUTABLE:
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
      case PrestoSqlParser::GRAPH:
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
      case PrestoSqlParser::OPTIMIZED:
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
      case PrestoSqlParser::WINDOW:
      case PrestoSqlParser::WORK:
      case PrestoSqlParser::WRITE:
      case PrestoSqlParser::YEAR:
      case PrestoSqlParser::ZONE:
      case PrestoSqlParser::IDENTIFIER:
      case PrestoSqlParser::DIGIT_IDENTIFIER:
      case PrestoSqlParser::QUOTED_IDENTIFIER:
      case PrestoSqlParser::BACKQUOTED_IDENTIFIER: {
        enterOuterAlt(_localctx, 4);
        setState(2140);
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

std::any PrestoSqlParser::QualifiedNameContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitQualifiedName(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::QualifiedNameContext* PrestoSqlParser::qualifiedName() {
  QualifiedNameContext* _localctx =
      _tracker.createInstance<QualifiedNameContext>(_ctx, getState());
  enterRule(_localctx, 154, PrestoSqlParser::RuleQualifiedName);

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
    setState(2143);
    identifier();
    setState(2148);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 274, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(2144);
        match(PrestoSqlParser::T__0);
        setState(2145);
        identifier();
      }
      setState(2150);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 274, _ctx);
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

std::any PrestoSqlParser::TableVersionContext::accept(
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
  enterRule(_localctx, 156, PrestoSqlParser::RuleTableVersionExpression);
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
    setState(2151);
    match(PrestoSqlParser::FOR);
    setState(2152);
    antlrcpp::downCast<TableVersionContext*>(_localctx)->tableVersionType =
        _input->LT(1);
    _la = _input->LA(1);
    if (!(((((_la - 196) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 196)) & 536871427) != 0))) {
      antlrcpp::downCast<TableVersionContext*>(_localctx)->tableVersionType =
          _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(2153);
    tableVersionState();
    setState(2154);
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

std::any PrestoSqlParser::TableversionbeforeContext::accept(
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

std::any PrestoSqlParser::TableversionasofContext::accept(
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
  enterRule(_localctx, 158, PrestoSqlParser::RuleTableVersionState);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2159);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::AS: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TableversionasofContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2156);
        match(PrestoSqlParser::AS);
        setState(2157);
        match(PrestoSqlParser::OF);
        break;
      }

      case PrestoSqlParser::BEFORE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::TableversionbeforeContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2158);
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

std::any PrestoSqlParser::CurrentUserGrantorContext::accept(
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

std::any PrestoSqlParser::SpecifiedPrincipalContext::accept(
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

std::any PrestoSqlParser::CurrentRoleGrantorContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitCurrentRoleGrantor(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::GrantorContext* PrestoSqlParser::grantor() {
  GrantorContext* _localctx =
      _tracker.createInstance<GrantorContext>(_ctx, getState());
  enterRule(_localctx, 160, PrestoSqlParser::RuleGrantor);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2164);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 276, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CurrentUserGrantorContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2161);
        match(PrestoSqlParser::CURRENT_USER);
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::CurrentRoleGrantorContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2162);
        match(PrestoSqlParser::CURRENT_ROLE);
        break;
      }

      case 3: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::SpecifiedPrincipalContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(2163);
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

std::any PrestoSqlParser::UnspecifiedPrincipalContext::accept(
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

std::any PrestoSqlParser::UserPrincipalContext::accept(
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

std::any PrestoSqlParser::RolePrincipalContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRolePrincipal(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::PrincipalContext* PrestoSqlParser::principal() {
  PrincipalContext* _localctx =
      _tracker.createInstance<PrincipalContext>(_ctx, getState());
  enterRule(_localctx, 162, PrestoSqlParser::RulePrincipal);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2171);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 277, _ctx)) {
      case 1: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UserPrincipalContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2166);
        match(PrestoSqlParser::USER);
        setState(2167);
        identifier();
        break;
      }

      case 2: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::RolePrincipalContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2168);
        match(PrestoSqlParser::ROLE);
        setState(2169);
        identifier();
        break;
      }

      case 3: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::UnspecifiedPrincipalContext>(
                    _localctx);
        enterOuterAlt(_localctx, 3);
        setState(2170);
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

std::any PrestoSqlParser::RolesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitRoles(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::RolesContext* PrestoSqlParser::roles() {
  RolesContext* _localctx =
      _tracker.createInstance<RolesContext>(_ctx, getState());
  enterRule(_localctx, 164, PrestoSqlParser::RuleRoles);
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
    setState(2173);
    identifier();
    setState(2178);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::T__3) {
      setState(2174);
      match(PrestoSqlParser::T__3);
      setState(2175);
      identifier();
      setState(2180);
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

std::any PrestoSqlParser::BackQuotedIdentifierContext::accept(
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

std::any PrestoSqlParser::QuotedIdentifierContext::accept(
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

std::any PrestoSqlParser::DigitIdentifierContext::accept(
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

std::any PrestoSqlParser::UnquotedIdentifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitUnquotedIdentifier(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::IdentifierContext* PrestoSqlParser::identifier() {
  IdentifierContext* _localctx =
      _tracker.createInstance<IdentifierContext>(_ctx, getState());
  enterRule(_localctx, 166, PrestoSqlParser::RuleIdentifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2186);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::IDENTIFIER: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UnquotedIdentifierContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2181);
        match(PrestoSqlParser::IDENTIFIER);
        break;
      }

      case PrestoSqlParser::QUOTED_IDENTIFIER: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::QuotedIdentifierContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2182);
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
      case PrestoSqlParser::EXCLUDE:
      case PrestoSqlParser::EXCLUDING:
      case PrestoSqlParser::EXECUTABLE:
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
      case PrestoSqlParser::GRAPH:
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
      case PrestoSqlParser::OPTIMIZED:
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
      case PrestoSqlParser::WINDOW:
      case PrestoSqlParser::WORK:
      case PrestoSqlParser::WRITE:
      case PrestoSqlParser::YEAR:
      case PrestoSqlParser::ZONE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::UnquotedIdentifierContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(2183);
        nonReserved();
        break;
      }

      case PrestoSqlParser::BACKQUOTED_IDENTIFIER: {
        _localctx =
            _tracker
                .createInstance<PrestoSqlParser::BackQuotedIdentifierContext>(
                    _localctx);
        enterOuterAlt(_localctx, 4);
        setState(2184);
        match(PrestoSqlParser::BACKQUOTED_IDENTIFIER);
        break;
      }

      case PrestoSqlParser::DIGIT_IDENTIFIER: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DigitIdentifierContext>(
                _localctx);
        enterOuterAlt(_localctx, 5);
        setState(2185);
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

std::any PrestoSqlParser::DecimalLiteralContext::accept(
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

std::any PrestoSqlParser::DoubleLiteralContext::accept(
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

std::any PrestoSqlParser::IntegerLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitIntegerLiteral(this);
  else
    return visitor->visitChildren(this);
}
PrestoSqlParser::NumberContext* PrestoSqlParser::number() {
  NumberContext* _localctx =
      _tracker.createInstance<NumberContext>(_ctx, getState());
  enterRule(_localctx, 168, PrestoSqlParser::RuleNumber);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2191);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::DECIMAL_VALUE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DecimalLiteralContext>(
                _localctx);
        enterOuterAlt(_localctx, 1);
        setState(2188);
        match(PrestoSqlParser::DECIMAL_VALUE);
        break;
      }

      case PrestoSqlParser::DOUBLE_VALUE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::DoubleLiteralContext>(
                _localctx);
        enterOuterAlt(_localctx, 2);
        setState(2189);
        match(PrestoSqlParser::DOUBLE_VALUE);
        break;
      }

      case PrestoSqlParser::INTEGER_VALUE: {
        _localctx =
            _tracker.createInstance<PrestoSqlParser::IntegerLiteralContext>(
                _localctx);
        enterOuterAlt(_localctx, 3);
        setState(2190);
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

std::any PrestoSqlParser::ConstraintSpecificationContext::accept(
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
  enterRule(_localctx, 170, PrestoSqlParser::RuleConstraintSpecification);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2195);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::CONSTRAINT: {
        enterOuterAlt(_localctx, 1);
        setState(2193);
        namedConstraintSpecification();
        break;
      }

      case PrestoSqlParser::PRIMARY:
      case PrestoSqlParser::UNIQUE: {
        enterOuterAlt(_localctx, 2);
        setState(2194);
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

std::any PrestoSqlParser::NamedConstraintSpecificationContext::accept(
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
  enterRule(_localctx, 172, PrestoSqlParser::RuleNamedConstraintSpecification);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2197);
    match(PrestoSqlParser::CONSTRAINT);
    setState(2198);
    antlrcpp::downCast<NamedConstraintSpecificationContext*>(_localctx)->name =
        identifier();
    setState(2199);
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

std::any PrestoSqlParser::UnnamedConstraintSpecificationContext::accept(
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
      _localctx, 174, PrestoSqlParser::RuleUnnamedConstraintSpecification);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(2201);
    constraintType();
    setState(2202);
    columnAliases();
    setState(2204);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 282, _ctx)) {
      case 1: {
        setState(2203);
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

std::any PrestoSqlParser::ConstraintTypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitConstraintType(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ConstraintTypeContext* PrestoSqlParser::constraintType() {
  ConstraintTypeContext* _localctx =
      _tracker.createInstance<ConstraintTypeContext>(_ctx, getState());
  enterRule(_localctx, 176, PrestoSqlParser::RuleConstraintType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2209);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::UNIQUE: {
        enterOuterAlt(_localctx, 1);
        setState(2206);
        match(PrestoSqlParser::UNIQUE);
        break;
      }

      case PrestoSqlParser::PRIMARY: {
        enterOuterAlt(_localctx, 2);
        setState(2207);
        match(PrestoSqlParser::PRIMARY);
        setState(2208);
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

std::any PrestoSqlParser::ConstraintQualifiersContext::accept(
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
  enterRule(_localctx, 178, PrestoSqlParser::RuleConstraintQualifiers);
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
    setState(2214);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while ((((_la & ~0x3fULL) == 0) &&
            ((1ULL << _la) & -6845471433603153920) != 0) ||
           _la == PrestoSqlParser::NOT

           || _la == PrestoSqlParser::RELY) {
      setState(2211);
      constraintQualifier();
      setState(2216);
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

std::any PrestoSqlParser::ConstraintQualifierContext::accept(
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
  enterRule(_localctx, 180, PrestoSqlParser::RuleConstraintQualifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2220);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 285, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(2217);
        constraintEnabled();
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(2218);
        constraintRely();
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(2219);
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

std::any PrestoSqlParser::ConstraintRelyContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitConstraintRely(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::ConstraintRelyContext* PrestoSqlParser::constraintRely() {
  ConstraintRelyContext* _localctx =
      _tracker.createInstance<ConstraintRelyContext>(_ctx, getState());
  enterRule(_localctx, 182, PrestoSqlParser::RuleConstraintRely);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2225);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::RELY: {
        enterOuterAlt(_localctx, 1);
        setState(2222);
        match(PrestoSqlParser::RELY);
        break;
      }

      case PrestoSqlParser::NOT: {
        enterOuterAlt(_localctx, 2);
        setState(2223);
        match(PrestoSqlParser::NOT);
        setState(2224);
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

std::any PrestoSqlParser::ConstraintEnabledContext::accept(
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
  enterRule(_localctx, 184, PrestoSqlParser::RuleConstraintEnabled);
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
    setState(2227);
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

std::any PrestoSqlParser::ConstraintEnforcedContext::accept(
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
  enterRule(_localctx, 186, PrestoSqlParser::RuleConstraintEnforced);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(2232);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case PrestoSqlParser::ENFORCED: {
        enterOuterAlt(_localctx, 1);
        setState(2229);
        match(PrestoSqlParser::ENFORCED);
        break;
      }

      case PrestoSqlParser::NOT: {
        enterOuterAlt(_localctx, 2);
        setState(2230);
        match(PrestoSqlParser::NOT);
        setState(2231);
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

tree::TerminalNode* PrestoSqlParser::NonReservedContext::EXECUTABLE() {
  return getToken(PrestoSqlParser::EXECUTABLE, 0);
}

tree::TerminalNode* PrestoSqlParser::NonReservedContext::EXCLUDE() {
  return getToken(PrestoSqlParser::EXCLUDE, 0);
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

tree::TerminalNode* PrestoSqlParser::NonReservedContext::GRAPH() {
  return getToken(PrestoSqlParser::GRAPH, 0);
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

tree::TerminalNode* PrestoSqlParser::NonReservedContext::OPTIMIZED() {
  return getToken(PrestoSqlParser::OPTIMIZED, 0);
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

tree::TerminalNode* PrestoSqlParser::NonReservedContext::WINDOW() {
  return getToken(PrestoSqlParser::WINDOW, 0);
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

std::any PrestoSqlParser::NonReservedContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<PrestoSqlVisitor*>(visitor))
    return parserVisitor->visitNonReserved(this);
  else
    return visitor->visitChildren(this);
}

PrestoSqlParser::NonReservedContext* PrestoSqlParser::nonReserved() {
  NonReservedContext* _localctx =
      _tracker.createInstance<NonReservedContext>(_ctx, getState());
  enterRule(_localctx, 188, PrestoSqlParser::RuleNonReserved);
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
    setState(2234);
    _la = _input->LA(1);
    if (!((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & -6508956968051886080) != 0) ||
          ((((_la - 66) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 66)) & -2335699536833519961) != 0) ||
          ((((_la - 130) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 130)) & -18163934272260209) != 0) ||
          ((((_la - 194) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 194)) & 2138211859951) != 0))) {
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
    case 39:
      return relationSempred(
          antlrcpp::downCast<RelationContext*>(context), predicateIndex);
    case 48:
      return booleanExpressionSempred(
          antlrcpp::downCast<BooleanExpressionContext*>(context),
          predicateIndex);
    case 50:
      return valueExpressionSempred(
          antlrcpp::downCast<ValueExpressionContext*>(context), predicateIndex);
    case 51:
      return primaryExpressionSempred(
          antlrcpp::downCast<PrimaryExpressionContext*>(context),
          predicateIndex);
    case 62:
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
      return precpred(_ctx, 15);
    case 10:
      return precpred(_ctx, 13);
    case 11:
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
    case 12:
      return precpred(_ctx, 6);

    default:
      break;
  }
  return true;
}

void PrestoSqlParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  prestosqlParserInitialize();
#else
  ::antlr4::internal::call_once(
      prestosqlParserOnceFlag, prestosqlParserInitialize);
#endif
}
