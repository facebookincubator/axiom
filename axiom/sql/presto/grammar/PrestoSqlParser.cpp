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
      4,    1,    262,  2064, 2,    0,    7,    0,    2,    1,    7,    1,
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
      1,    0,    1,    0,    1,    0,    1,    1,    1,    1,    1,    1,
      1,    2,    1,    2,    1,    2,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    201,  8,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    206,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    212,  8,
      3,    1,    3,    1,    3,    3,    3,    216,  8,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,    3,
      230,  8,    3,    1,    3,    1,    3,    3,    3,    234,  8,    3,
      1,    3,    1,    3,    3,    3,    238,  8,    3,    1,    3,    1,
      3,    3,    3,    242,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    250,  8,    3,    1,
      3,    1,    3,    3,    3,    254,  8,    3,    1,    3,    3,    3,
      257,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    3,    3,    264,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    5,    3,    271,  8,    3,    10,   3,    12,
      3,    274,  9,    3,    1,    3,    1,    3,    1,    3,    3,    3,
      279,  8,    3,    1,    3,    1,    3,    3,    3,    283,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    289,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,
      3,    296,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    305,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    3,    3,    314,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      3,    3,    325,  8,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    332,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      3,    3,    342,  8,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    349,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    357,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    3,    3,    365,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    373,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    383,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    390,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,
      3,    398,  8,    3,    1,    3,    1,    3,    1,    3,    3,    3,
      403,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    414,
      8,    3,    1,    3,    1,    3,    1,    3,    3,    3,    419,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    430,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    441,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    5,    3,    451,  8,    3,    10,   3,    12,   3,
      454,  9,    3,    1,    3,    1,    3,    1,    3,    3,    3,    459,
      8,    3,    1,    3,    1,    3,    1,    3,    3,    3,    464,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    470,
      8,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    3,    3,    479,  8,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    490,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,    3,
      499,  8,    3,    1,    3,    1,    3,    1,    3,    3,    3,    504,
      8,    3,    1,    3,    1,    3,    3,    3,    508,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,
      3,    516,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    3,    3,    523,  8,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    3,    3,    536,  8,    3,    1,    3,
      3,    3,    539,  8,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    5,    3,    547,  8,    3,    10,   3,
      12,   3,    550,  9,    3,    3,    3,    552,  8,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    559,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    568,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    3,    3,    574,  8,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    579,  8,    3,    1,    3,    1,    3,
      3,    3,    583,  8,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    5,    3,    591,  8,    3,    10,   3,
      12,   3,    594,  9,    3,    3,    3,    596,  8,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    3,    3,    606,  8,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    5,    3,    617,  8,    3,    10,   3,    12,   3,    620,  9,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    625,  8,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    630,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    636,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    5,    3,
      643,  8,    3,    10,   3,    12,   3,    646,  9,    3,    1,    3,
      1,    3,    1,    3,    3,    3,    651,  8,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    658,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    5,    3,    664,  8,
      3,    10,   3,    12,   3,    667,  9,    3,    1,    3,    1,    3,
      3,    3,    671,  8,    3,    1,    3,    1,    3,    3,    3,    675,
      8,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    3,    3,    683,  8,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    689,  8,    3,    1,    3,    1,    3,
      1,    3,    5,    3,    694,  8,    3,    10,   3,    12,   3,    697,
      9,    3,    1,    3,    1,    3,    3,    3,    701,  8,    3,    1,
      3,    1,    3,    3,    3,    705,  8,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      3,    3,    715,  8,    3,    1,    3,    3,    3,    718,  8,    3,
      1,    3,    1,    3,    3,    3,    722,  8,    3,    1,    3,    3,
      3,    725,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      5,    3,    731,  8,    3,    10,   3,    12,   3,    734,  9,    3,
      1,    3,    1,    3,    3,    3,    738,  8,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    3,
      3,    759,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      3,    3,    765,  8,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    3,    3,    771,  8,    3,    3,    3,    773,  8,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    3,    3,    779,  8,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    3,    3,    785,  8,
      3,    3,    3,    787,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    3,    3,    795,  8,    3,    3,
      3,    797,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    3,    3,    816,  8,    3,    1,    3,    1,    3,    1,
      3,    3,    3,    821,  8,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    3,    3,    828,  8,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    3,    3,    840,  8,    3,    3,    3,
      842,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    3,    3,    850,  8,    3,    3,    3,    852,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    5,    3,    868,  8,    3,    10,   3,
      12,   3,    871,  9,    3,    3,    3,    873,  8,    3,    1,    3,
      1,    3,    3,    3,    877,  8,    3,    1,    3,    1,    3,    3,
      3,    881,  8,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,
      1,    3,    1,    3,    1,    3,    1,    3,    5,    3,    897,  8,
      3,    10,   3,    12,   3,    900,  9,    3,    3,    3,    902,  8,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    1,    3,    1,    3,    1,    3,    1,    3,    1,    3,    1,
      3,    5,    3,    916,  8,    3,    10,   3,    12,   3,    919,  9,
      3,    1,    3,    1,    3,    3,    3,    923,  8,    3,    3,    3,
      925,  8,    3,    1,    4,    3,    4,    928,  8,    4,    1,    4,
      1,    4,    1,    5,    1,    5,    3,    5,    934,  8,    5,    1,
      5,    1,    5,    1,    5,    5,    5,    939,  8,    5,    10,   5,
      12,   5,    942,  9,    5,    1,    6,    1,    6,    1,    6,    3,
      6,    947,  8,    6,    1,    7,    1,    7,    1,    7,    1,    7,
      3,    7,    953,  8,    7,    1,    7,    1,    7,    3,    7,    957,
      8,    7,    1,    7,    1,    7,    3,    7,    961,  8,    7,    1,
      8,    1,    8,    1,    8,    1,    8,    3,    8,    967,  8,    8,
      1,    9,    1,    9,    1,    9,    1,    9,    5,    9,    973,  8,
      9,    10,   9,    12,   9,    976,  9,    9,    1,    9,    1,    9,
      1,    10,   1,    10,   1,    10,   1,    10,   1,    11,   1,    11,
      1,    11,   1,    12,   5,    12,   988,  8,    12,   10,   12,   12,
      12,   991,  9,    12,   1,    13,   1,    13,   1,    13,   1,    13,
      3,    13,   997,  8,    13,   1,    14,   5,    14,   1000, 8,    14,
      10,   14,   12,   14,   1003, 9,    14,   1,    15,   1,    15,   1,
      16,   1,    16,   3,    16,   1009, 8,    16,   1,    17,   1,    17,
      1,    17,   1,    18,   1,    18,   1,    18,   3,    18,   1017, 8,
      18,   1,    19,   1,    19,   3,    19,   1021, 8,    19,   1,    20,
      1,    20,   1,    20,   3,    20,   1026, 8,    20,   1,    21,   1,
      21,   1,    21,   1,    21,   1,    21,   1,    21,   1,    21,   1,
      21,   1,    21,   3,    21,   1037, 8,    21,   1,    22,   1,    22,
      1,    23,   1,    23,   1,    23,   1,    23,   1,    23,   1,    23,
      5,    23,   1047, 8,    23,   10,   23,   12,   23,   1050, 9,    23,
      3,    23,   1052, 8,    23,   1,    23,   1,    23,   1,    23,   3,
      23,   1057, 8,    23,   3,    23,   1059, 8,    23,   1,    23,   1,
      23,   1,    23,   1,    23,   1,    23,   1,    23,   1,    23,   3,
      23,   1068, 8,    23,   3,    23,   1070, 8,    23,   1,    24,   1,
      24,   1,    24,   1,    24,   1,    24,   1,    24,   3,    24,   1078,
      8,    24,   1,    24,   1,    24,   1,    24,   1,    24,   3,    24,
      1084, 8,    24,   1,    24,   5,    24,   1087, 8,    24,   10,   24,
      12,   24,   1090, 9,    24,   1,    25,   1,    25,   1,    25,   1,
      25,   1,    25,   1,    25,   1,    25,   5,    25,   1099, 8,    25,
      10,   25,   12,   25,   1102, 9,    25,   1,    25,   1,    25,   1,
      25,   1,    25,   3,    25,   1108, 8,    25,   1,    26,   1,    26,
      3,    26,   1112, 8,    26,   1,    26,   1,    26,   3,    26,   1116,
      8,    26,   1,    27,   1,    27,   3,    27,   1120, 8,    27,   1,
      27,   1,    27,   1,    27,   5,    27,   1125, 8,    27,   10,   27,
      12,   27,   1128, 9,    27,   1,    27,   1,    27,   1,    27,   1,
      27,   5,    27,   1134, 8,    27,   10,   27,   12,   27,   1137, 9,
      27,   3,    27,   1139, 8,    27,   1,    27,   1,    27,   3,    27,
      1143, 8,    27,   1,    27,   1,    27,   1,    27,   3,    27,   1148,
      8,    27,   1,    27,   1,    27,   3,    27,   1152, 8,    27,   1,
      28,   3,    28,   1155, 8,    28,   1,    28,   1,    28,   1,    28,
      5,    28,   1160, 8,    28,   10,   28,   12,   28,   1163, 9,    28,
      1,    29,   1,    29,   1,    29,   1,    29,   1,    29,   1,    29,
      5,    29,   1171, 8,    29,   10,   29,   12,   29,   1174, 9,    29,
      3,    29,   1176, 8,    29,   1,    29,   1,    29,   1,    29,   1,
      29,   1,    29,   1,    29,   5,    29,   1184, 8,    29,   10,   29,
      12,   29,   1187, 9,    29,   3,    29,   1189, 8,    29,   1,    29,
      1,    29,   1,    29,   1,    29,   1,    29,   1,    29,   1,    29,
      5,    29,   1198, 8,    29,   10,   29,   12,   29,   1201, 9,    29,
      1,    29,   1,    29,   3,    29,   1205, 8,    29,   1,    30,   1,
      30,   1,    30,   1,    30,   5,    30,   1211, 8,    30,   10,   30,
      12,   30,   1214, 9,    30,   3,    30,   1216, 8,    30,   1,    30,
      1,    30,   3,    30,   1220, 8,    30,   1,    31,   1,    31,   3,
      31,   1224, 8,    31,   1,    31,   1,    31,   1,    31,   1,    31,
      1,    31,   1,    32,   1,    32,   1,    33,   1,    33,   3,    33,
      1235, 8,    33,   1,    33,   3,    33,   1238, 8,    33,   1,    33,
      1,    33,   1,    33,   1,    33,   1,    33,   3,    33,   1245, 8,
      33,   1,    34,   1,    34,   1,    34,   1,    34,   1,    34,   1,
      34,   1,    34,   1,    34,   1,    34,   1,    34,   1,    34,   1,
      34,   1,    34,   1,    34,   1,    34,   1,    34,   1,    34,   3,
      34,   1264, 8,    34,   5,    34,   1266, 8,    34,   10,   34,   12,
      34,   1269, 9,    34,   1,    35,   3,    35,   1272, 8,    35,   1,
      35,   1,    35,   3,    35,   1276, 8,    35,   1,    35,   1,    35,
      3,    35,   1280, 8,    35,   1,    35,   1,    35,   3,    35,   1284,
      8,    35,   3,    35,   1286, 8,    35,   1,    36,   1,    36,   1,
      36,   1,    36,   1,    36,   1,    36,   1,    36,   5,    36,   1295,
      8,    36,   10,   36,   12,   36,   1298, 9,    36,   1,    36,   1,
      36,   3,    36,   1302, 8,    36,   1,    37,   1,    37,   1,    37,
      1,    37,   1,    37,   1,    37,   1,    37,   3,    37,   1311, 8,
      37,   1,    38,   1,    38,   1,    39,   1,    39,   3,    39,   1317,
      8,    39,   1,    39,   1,    39,   3,    39,   1321, 8,    39,   3,
      39,   1323, 8,    39,   1,    40,   1,    40,   1,    40,   1,    40,
      5,    40,   1329, 8,    40,   10,   40,   12,   40,   1332, 9,    40,
      1,    40,   1,    40,   1,    41,   1,    41,   3,    41,   1338, 8,
      41,   1,    41,   1,    41,   1,    41,   1,    41,   1,    41,   1,
      41,   1,    41,   1,    41,   1,    41,   5,    41,   1349, 8,    41,
      10,   41,   12,   41,   1352, 9,    41,   1,    41,   1,    41,   1,
      41,   3,    41,   1357, 8,    41,   1,    41,   1,    41,   1,    41,
      1,    41,   1,    41,   1,    41,   1,    41,   1,    41,   1,    41,
      3,    41,   1368, 8,    41,   1,    42,   1,    42,   1,    43,   1,
      43,   1,    43,   3,    43,   1375, 8,    43,   1,    43,   1,    43,
      3,    43,   1379, 8,    43,   1,    43,   1,    43,   1,    43,   1,
      43,   1,    43,   1,    43,   5,    43,   1387, 8,    43,   10,   43,
      12,   43,   1390, 9,    43,   1,    44,   1,    44,   1,    44,   1,
      44,   1,    44,   1,    44,   1,    44,   1,    44,   1,    44,   1,
      44,   3,    44,   1402, 8,    44,   1,    44,   1,    44,   1,    44,
      1,    44,   1,    44,   1,    44,   3,    44,   1410, 8,    44,   1,
      44,   1,    44,   1,    44,   1,    44,   1,    44,   5,    44,   1417,
      8,    44,   10,   44,   12,   44,   1420, 9,    44,   1,    44,   1,
      44,   1,    44,   3,    44,   1425, 8,    44,   1,    44,   1,    44,
      1,    44,   1,    44,   1,    44,   1,    44,   3,    44,   1433, 8,
      44,   1,    44,   1,    44,   1,    44,   1,    44,   3,    44,   1439,
      8,    44,   1,    44,   1,    44,   3,    44,   1443, 8,    44,   1,
      44,   1,    44,   1,    44,   3,    44,   1448, 8,    44,   1,    44,
      1,    44,   1,    44,   3,    44,   1453, 8,    44,   1,    45,   1,
      45,   1,    45,   1,    45,   3,    45,   1459, 8,    45,   1,    45,
      1,    45,   1,    45,   1,    45,   1,    45,   1,    45,   1,    45,
      1,    45,   1,    45,   1,    45,   1,    45,   1,    45,   5,    45,
      1473, 8,    45,   10,   45,   12,   45,   1476, 9,    45,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   4,    46,
      1502, 8,    46,   11,   46,   12,   46,   1503, 1,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   5,    46,
      1513, 8,    46,   10,   46,   12,   46,   1516, 9,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      3,    46,   1525, 8,    46,   1,    46,   3,    46,   1528, 8,    46,
      1,    46,   1,    46,   1,    46,   3,    46,   1533, 8,    46,   1,
      46,   1,    46,   1,    46,   5,    46,   1538, 8,    46,   10,   46,
      12,   46,   1541, 9,    46,   3,    46,   1543, 8,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   5,    46,   1550, 8,
      46,   10,   46,   12,   46,   1553, 9,    46,   3,    46,   1555, 8,
      46,   1,    46,   1,    46,   3,    46,   1559, 8,    46,   1,    46,
      3,    46,   1562, 8,    46,   1,    46,   3,    46,   1565, 8,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   1,    46,   5,    46,   1575, 8,    46,   10,   46,   12,
      46,   1578, 9,    46,   3,    46,   1580, 8,    46,   1,    46,   1,
      46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,
      46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,
      46,   1,    46,   4,    46,   1597, 8,    46,   11,   46,   12,   46,
      1598, 1,    46,   1,    46,   3,    46,   1603, 8,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   4,    46,   1609, 8,    46,   11,
      46,   12,   46,   1610, 1,    46,   1,    46,   3,    46,   1615, 8,
      46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,
      46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,
      46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,
      46,   1,    46,   1,    46,   1,    46,   5,    46,   1638, 8,    46,
      10,   46,   12,   46,   1641, 9,    46,   3,    46,   1643, 8,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   3,    46,   1652, 8,    46,   1,    46,   1,    46,   1,
      46,   1,    46,   3,    46,   1658, 8,    46,   1,    46,   1,    46,
      1,    46,   1,    46,   3,    46,   1664, 8,    46,   1,    46,   1,
      46,   1,    46,   1,    46,   3,    46,   1670, 8,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   3,    46,   1680, 8,    46,   1,    46,   1,    46,   1,
      46,   1,    46,   1,    46,   1,    46,   1,    46,   3,    46,   1689,
      8,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   5,    46,   1709, 8,    46,   10,   46,   12,   46,   1712,
      9,    46,   3,    46,   1714, 8,    46,   1,    46,   3,    46,   1717,
      8,    46,   1,    46,   1,    46,   1,    46,   1,    46,   1,    46,
      1,    46,   1,    46,   1,    46,   5,    46,   1727, 8,    46,   10,
      46,   12,   46,   1730, 9,    46,   1,    47,   1,    47,   1,    47,
      1,    47,   3,    47,   1736, 8,    47,   3,    47,   1738, 8,    47,
      1,    48,   1,    48,   1,    48,   1,    48,   3,    48,   1744, 8,
      48,   1,    49,   1,    49,   1,    49,   1,    49,   1,    49,   1,
      49,   3,    49,   1752, 8,    49,   1,    50,   1,    50,   1,    51,
      1,    51,   1,    52,   1,    52,   1,    53,   1,    53,   3,    53,
      1762, 8,    53,   1,    53,   1,    53,   1,    53,   1,    53,   3,
      53,   1768, 8,    53,   1,    54,   1,    54,   1,    55,   1,    55,
      1,    56,   1,    56,   1,    56,   1,    56,   5,    56,   1778, 8,
      56,   10,   56,   12,   56,   1781, 9,    56,   3,    56,   1783, 8,
      56,   1,    56,   1,    56,   1,    57,   1,    57,   1,    57,   1,
      57,   1,    57,   1,    57,   1,    57,   1,    57,   1,    57,   1,
      57,   1,    57,   1,    57,   1,    57,   1,    57,   1,    57,   1,
      57,   1,    57,   1,    57,   1,    57,   1,    57,   1,    57,   5,
      57,   1808, 8,    57,   10,   57,   12,   57,   1811, 9,    57,   1,
      57,   1,    57,   1,    57,   1,    57,   1,    57,   1,    57,   1,
      57,   5,    57,   1820, 8,    57,   10,   57,   12,   57,   1823, 9,
      57,   1,    57,   1,    57,   3,    57,   1827, 8,    57,   1,    57,
      1,    57,   1,    57,   1,    57,   1,    57,   3,    57,   1834, 8,
      57,   1,    57,   1,    57,   5,    57,   1838, 8,    57,   10,   57,
      12,   57,   1841, 9,    57,   1,    58,   1,    58,   3,    58,   1845,
      8,    58,   1,    59,   1,    59,   1,    59,   1,    59,   3,    59,
      1851, 8,    59,   1,    60,   1,    60,   1,    60,   1,    60,   1,
      60,   1,    61,   1,    61,   1,    61,   1,    61,   1,    61,   1,
      61,   1,    62,   1,    62,   1,    62,   1,    62,   1,    62,   1,
      62,   1,    62,   5,    62,   1871, 8,    62,   10,   62,   12,   62,
      1874, 9,    62,   3,    62,   1876, 8,    62,   1,    62,   1,    62,
      1,    62,   1,    62,   1,    62,   5,    62,   1883, 8,    62,   10,
      62,   12,   62,   1886, 9,    62,   3,    62,   1888, 8,    62,   1,
      62,   3,    62,   1891, 8,    62,   1,    62,   1,    62,   1,    63,
      1,    63,   1,    63,   1,    63,   1,    63,   1,    63,   1,    63,
      1,    63,   1,    63,   1,    63,   1,    63,   1,    63,   1,    63,
      1,    63,   1,    63,   1,    63,   1,    63,   1,    63,   1,    63,
      1,    63,   1,    63,   1,    63,   1,    63,   1,    63,   3,    63,
      1919, 8,    63,   1,    64,   1,    64,   1,    64,   1,    64,   1,
      64,   1,    64,   1,    64,   1,    64,   1,    64,   3,    64,   1930,
      8,    64,   1,    65,   1,    65,   1,    65,   1,    65,   1,    66,
      1,    66,   1,    66,   1,    66,   3,    66,   1940, 8,    66,   1,
      67,   1,    67,   1,    67,   1,    67,   1,    67,   3,    67,   1947,
      8,    67,   1,    68,   1,    68,   1,    68,   1,    68,   1,    68,
      1,    68,   1,    68,   3,    68,   1956, 8,    68,   1,    69,   1,
      69,   1,    69,   1,    69,   1,    69,   3,    69,   1963, 8,    69,
      1,    70,   1,    70,   1,    70,   1,    70,   3,    70,   1969, 8,
      70,   1,    71,   1,    71,   1,    71,   5,    71,   1974, 8,    71,
      10,   71,   12,   71,   1977, 9,    71,   1,    72,   1,    72,   1,
      72,   1,    72,   1,    72,   1,    73,   1,    73,   1,    73,   3,
      73,   1987, 8,    73,   1,    74,   1,    74,   1,    74,   3,    74,
      1992, 8,    74,   1,    75,   1,    75,   1,    75,   1,    75,   1,
      75,   3,    75,   1999, 8,    75,   1,    76,   1,    76,   1,    76,
      5,    76,   2004, 8,    76,   10,   76,   12,   76,   2007, 9,    76,
      1,    77,   1,    77,   1,    77,   1,    77,   1,    77,   3,    77,
      2014, 8,    77,   1,    78,   1,    78,   1,    78,   3,    78,   2019,
      8,    78,   1,    79,   1,    79,   3,    79,   2023, 8,    79,   1,
      80,   1,    80,   1,    80,   1,    80,   1,    81,   1,    81,   1,
      81,   3,    81,   2032, 8,    81,   1,    82,   1,    82,   1,    82,
      3,    82,   2037, 8,    82,   1,    83,   5,    83,   2040, 8,    83,
      10,   83,   12,   83,   2043, 9,    83,   1,    84,   1,    84,   1,
      84,   3,    84,   2048, 8,    84,   1,    85,   1,    85,   1,    85,
      3,    85,   2053, 8,    85,   1,    86,   1,    86,   1,    87,   1,
      87,   1,    87,   3,    87,   2060, 8,    87,   1,    88,   1,    88,
      1,    88,   0,    6,    48,   68,   86,   90,   92,   114,  89,   0,
      2,    4,    6,    8,    10,   12,   14,   16,   18,   20,   22,   24,
      26,   28,   30,   32,   34,   36,   38,   40,   42,   44,   46,   48,
      50,   52,   54,   56,   58,   60,   62,   64,   66,   68,   70,   72,
      74,   76,   78,   80,   82,   84,   86,   88,   90,   92,   94,   96,
      98,   100,  102,  104,  106,  108,  110,  112,  114,  116,  118,  120,
      122,  124,  126,  128,  130,  132,  134,  136,  138,  140,  142,  144,
      146,  148,  150,  152,  154,  156,  158,  160,  162,  164,  166,  168,
      170,  172,  174,  176,  0,    25,   2,    0,    28,   28,   168,  168,
      2,    0,    51,   51,   104,  104,  2,    0,    80,   80,   96,   96,
      2,    0,    66,   66,   97,   97,   1,    0,    177,  178,  2,    0,
      13,   13,   248,  248,  2,    0,    65,   65,   214,  214,  2,    0,
      20,   20,   53,   53,   2,    0,    76,   76,   112,  112,  2,    0,
      13,   13,   57,   57,   2,    0,    23,   23,   194,  194,  1,    0,
      239,  240,  1,    0,    241,  243,  1,    0,    233,  238,  3,    0,
      13,   13,   17,   17,   189,  189,  2,    0,    73,   73,   207,  207,
      5,    0,    49,   49,   93,   93,   123,  124,  181,  181,  231,  231,
      1,    0,    127,  130,  2,    0,    77,   77,   153,  153,  3,    0,
      88,   88,   108,  108,  201,  201,  7,    0,    58,   58,   67,   67,
      87,   87,   105,  105,  120,  120,  142,  142,  221,  221,  2,    0,
      141,  141,  230,  230,  3,    0,    195,  196,  204,  204,  224,  224,
      2,    0,    56,   56,   61,   61,   51,   0,    11,   13,   15,   15,
      17,   18,   20,   23,   26,   28,   31,   36,   41,   41,   43,   43,
      47,   49,   51,   51,   53,   53,   55,   56,   58,   58,   61,   61,
      63,   63,   66,   67,   70,   70,   72,   72,   74,   77,   79,   79,
      82,   88,   91,   91,   93,   95,   97,   97,   99,   99,   102,  102,
      104,  105,  107,  108,  110,  113,  115,  115,  117,  117,  120,  125,
      127,  132,  136,  139,  141,  143,  146,  146,  148,  153,  155,  159,
      161,  171,  173,  175,  177,  182,  184,  196,  198,  201,  203,  206,
      208,  210,  212,  213,  215,  215,  217,  219,  221,  221,  223,  225,
      229,  232,  2380, 0,    178,  1,    0,    0,    0,    2,    181,  1,
      0,    0,    0,    4,    184,  1,    0,    0,    0,    6,    924,  1,
      0,    0,    0,    8,    927,  1,    0,    0,    0,    10,   931,  1,
      0,    0,    0,    12,   946,  1,    0,    0,    0,    14,   948,  1,
      0,    0,    0,    16,   962,  1,    0,    0,    0,    18,   968,  1,
      0,    0,    0,    20,   979,  1,    0,    0,    0,    22,   983,  1,
      0,    0,    0,    24,   989,  1,    0,    0,    0,    26,   996,  1,
      0,    0,    0,    28,   1001, 1,    0,    0,    0,    30,   1004, 1,
      0,    0,    0,    32,   1008, 1,    0,    0,    0,    34,   1010, 1,
      0,    0,    0,    36,   1013, 1,    0,    0,    0,    38,   1020, 1,
      0,    0,    0,    40,   1025, 1,    0,    0,    0,    42,   1036, 1,
      0,    0,    0,    44,   1038, 1,    0,    0,    0,    46,   1040, 1,
      0,    0,    0,    48,   1071, 1,    0,    0,    0,    50,   1107, 1,
      0,    0,    0,    52,   1109, 1,    0,    0,    0,    54,   1117, 1,
      0,    0,    0,    56,   1154, 1,    0,    0,    0,    58,   1204, 1,
      0,    0,    0,    60,   1219, 1,    0,    0,    0,    62,   1221, 1,
      0,    0,    0,    64,   1230, 1,    0,    0,    0,    66,   1244, 1,
      0,    0,    0,    68,   1246, 1,    0,    0,    0,    70,   1285, 1,
      0,    0,    0,    72,   1301, 1,    0,    0,    0,    74,   1303, 1,
      0,    0,    0,    76,   1312, 1,    0,    0,    0,    78,   1314, 1,
      0,    0,    0,    80,   1324, 1,    0,    0,    0,    82,   1367, 1,
      0,    0,    0,    84,   1369, 1,    0,    0,    0,    86,   1378, 1,
      0,    0,    0,    88,   1452, 1,    0,    0,    0,    90,   1458, 1,
      0,    0,    0,    92,   1716, 1,    0,    0,    0,    94,   1737, 1,
      0,    0,    0,    96,   1743, 1,    0,    0,    0,    98,   1751, 1,
      0,    0,    0,    100,  1753, 1,    0,    0,    0,    102,  1755, 1,
      0,    0,    0,    104,  1757, 1,    0,    0,    0,    106,  1759, 1,
      0,    0,    0,    108,  1769, 1,    0,    0,    0,    110,  1771, 1,
      0,    0,    0,    112,  1773, 1,    0,    0,    0,    114,  1833, 1,
      0,    0,    0,    116,  1844, 1,    0,    0,    0,    118,  1850, 1,
      0,    0,    0,    120,  1852, 1,    0,    0,    0,    122,  1857, 1,
      0,    0,    0,    124,  1863, 1,    0,    0,    0,    126,  1918, 1,
      0,    0,    0,    128,  1929, 1,    0,    0,    0,    130,  1931, 1,
      0,    0,    0,    132,  1939, 1,    0,    0,    0,    134,  1946, 1,
      0,    0,    0,    136,  1955, 1,    0,    0,    0,    138,  1962, 1,
      0,    0,    0,    140,  1968, 1,    0,    0,    0,    142,  1970, 1,
      0,    0,    0,    144,  1978, 1,    0,    0,    0,    146,  1986, 1,
      0,    0,    0,    148,  1991, 1,    0,    0,    0,    150,  1998, 1,
      0,    0,    0,    152,  2000, 1,    0,    0,    0,    154,  2013, 1,
      0,    0,    0,    156,  2018, 1,    0,    0,    0,    158,  2022, 1,
      0,    0,    0,    160,  2024, 1,    0,    0,    0,    162,  2028, 1,
      0,    0,    0,    164,  2036, 1,    0,    0,    0,    166,  2041, 1,
      0,    0,    0,    168,  2047, 1,    0,    0,    0,    170,  2052, 1,
      0,    0,    0,    172,  2054, 1,    0,    0,    0,    174,  2059, 1,
      0,    0,    0,    176,  2061, 1,    0,    0,    0,    178,  179,  3,
      6,    3,    0,    179,  180,  5,    0,    0,    1,    180,  1,    1,
      0,    0,    0,    181,  182,  3,    84,   42,   0,    182,  183,  5,
      0,    0,    1,    183,  3,    1,    0,    0,    0,    184,  185,  3,
      32,   16,   0,    185,  186,  5,    0,    0,    1,    186,  5,    1,
      0,    0,    0,    187,  925,  3,    8,    4,    0,    188,  189,  5,
      218,  0,    0,    189,  925,  3,    154,  77,   0,    190,  191,  5,
      218,  0,    0,    191,  192,  3,    154,  77,   0,    192,  193,  5,
      1,    0,    0,    193,  194,  3,    154,  77,   0,    194,  925,  1,
      0,    0,    0,    195,  196,  5,    38,   0,    0,    196,  200,  5,
      179,  0,    0,    197,  198,  5,    94,   0,    0,    198,  199,  5,
      134,  0,    0,    199,  201,  5,    69,   0,    0,    200,  197,  1,
      0,    0,    0,    200,  201,  1,    0,    0,    0,    201,  202,  1,
      0,    0,    0,    202,  205,  3,    142,  71,   0,    203,  204,  5,
      228,  0,    0,    204,  206,  3,    18,   9,    0,    205,  203,  1,
      0,    0,    0,    205,  206,  1,    0,    0,    0,    206,  925,  1,
      0,    0,    0,    207,  208,  5,    59,   0,    0,    208,  211,  5,
      179,  0,    0,    209,  210,  5,    94,   0,    0,    210,  212,  5,
      69,   0,    0,    211,  209,  1,    0,    0,    0,    211,  212,  1,
      0,    0,    0,    212,  213,  1,    0,    0,    0,    213,  215,  3,
      142,  71,   0,    214,  216,  7,    0,    0,    0,    215,  214,  1,
      0,    0,    0,    215,  216,  1,    0,    0,    0,    216,  925,  1,
      0,    0,    0,    217,  218,  5,    14,   0,    0,    218,  219,  5,
      179,  0,    0,    219,  220,  3,    142,  71,   0,    220,  221,  5,
      163,  0,    0,    221,  222,  5,    205,  0,    0,    222,  223,  3,
      154,  77,   0,    223,  925,  1,    0,    0,    0,    224,  225,  5,
      38,   0,    0,    225,  229,  5,    197,  0,    0,    226,  227,  5,
      94,   0,    0,    227,  228,  5,    134,  0,    0,    228,  230,  5,
      69,   0,    0,    229,  226,  1,    0,    0,    0,    229,  230,  1,
      0,    0,    0,    230,  231,  1,    0,    0,    0,    231,  233,  3,
      142,  71,   0,    232,  234,  3,    80,   40,   0,    233,  232,  1,
      0,    0,    0,    233,  234,  1,    0,    0,    0,    234,  237,  1,
      0,    0,    0,    235,  236,  5,    34,   0,    0,    236,  238,  3,
      94,   47,   0,    237,  235,  1,    0,    0,    0,    237,  238,  1,
      0,    0,    0,    238,  241,  1,    0,    0,    0,    239,  240,  5,
      228,  0,    0,    240,  242,  3,    18,   9,    0,    241,  239,  1,
      0,    0,    0,    241,  242,  1,    0,    0,    0,    242,  243,  1,
      0,    0,    0,    243,  249,  5,    19,   0,    0,    244,  250,  3,
      8,    4,    0,    245,  246,  5,    2,    0,    0,    246,  247,  3,
      8,    4,    0,    247,  248,  5,    3,    0,    0,    248,  250,  1,
      0,    0,    0,    249,  244,  1,    0,    0,    0,    249,  245,  1,
      0,    0,    0,    250,  256,  1,    0,    0,    0,    251,  253,  5,
      228,  0,    0,    252,  254,  5,    131,  0,    0,    253,  252,  1,
      0,    0,    0,    253,  254,  1,    0,    0,    0,    254,  255,  1,
      0,    0,    0,    255,  257,  5,    47,   0,    0,    256,  251,  1,
      0,    0,    0,    256,  257,  1,    0,    0,    0,    257,  925,  1,
      0,    0,    0,    258,  259,  5,    38,   0,    0,    259,  263,  5,
      197,  0,    0,    260,  261,  5,    94,   0,    0,    261,  262,  5,
      134,  0,    0,    262,  264,  5,    69,   0,    0,    263,  260,  1,
      0,    0,    0,    263,  264,  1,    0,    0,    0,    264,  265,  1,
      0,    0,    0,    265,  266,  3,    142,  71,   0,    266,  267,  5,
      2,    0,    0,    267,  272,  3,    12,   6,    0,    268,  269,  5,
      4,    0,    0,    269,  271,  3,    12,   6,    0,    270,  268,  1,
      0,    0,    0,    271,  274,  1,    0,    0,    0,    272,  270,  1,
      0,    0,    0,    272,  273,  1,    0,    0,    0,    273,  275,  1,
      0,    0,    0,    274,  272,  1,    0,    0,    0,    275,  278,  5,
      3,    0,    0,    276,  277,  5,    34,   0,    0,    277,  279,  3,
      94,   47,   0,    278,  276,  1,    0,    0,    0,    278,  279,  1,
      0,    0,    0,    279,  282,  1,    0,    0,    0,    280,  281,  5,
      228,  0,    0,    281,  283,  3,    18,   9,    0,    282,  280,  1,
      0,    0,    0,    282,  283,  1,    0,    0,    0,    283,  925,  1,
      0,    0,    0,    284,  285,  5,    59,   0,    0,    285,  288,  5,
      197,  0,    0,    286,  287,  5,    94,   0,    0,    287,  289,  5,
      69,   0,    0,    288,  286,  1,    0,    0,    0,    288,  289,  1,
      0,    0,    0,    289,  290,  1,    0,    0,    0,    290,  925,  3,
      142,  71,   0,    291,  292,  5,    100,  0,    0,    292,  293,  5,
      103,  0,    0,    293,  295,  3,    142,  71,   0,    294,  296,  3,
      80,   40,   0,    295,  294,  1,    0,    0,    0,    295,  296,  1,
      0,    0,    0,    296,  297,  1,    0,    0,    0,    297,  298,  3,
      8,    4,    0,    298,  925,  1,    0,    0,    0,    299,  300,  5,
      52,   0,    0,    300,  301,  5,    80,   0,    0,    301,  304,  3,
      142,  71,   0,    302,  303,  5,    227,  0,    0,    303,  305,  3,
      86,   43,   0,    304,  302,  1,    0,    0,    0,    304,  305,  1,
      0,    0,    0,    305,  925,  1,    0,    0,    0,    306,  307,  5,
      208,  0,    0,    307,  308,  5,    197,  0,    0,    308,  925,  3,
      142,  71,   0,    309,  310,  5,    14,   0,    0,    310,  313,  5,
      197,  0,    0,    311,  312,  5,    94,   0,    0,    312,  314,  5,
      69,   0,    0,    313,  311,  1,    0,    0,    0,    313,  314,  1,
      0,    0,    0,    314,  315,  1,    0,    0,    0,    315,  316,  3,
      142,  71,   0,    316,  317,  5,    163,  0,    0,    317,  318,  5,
      205,  0,    0,    318,  319,  3,    142,  71,   0,    319,  925,  1,
      0,    0,    0,    320,  321,  5,    14,   0,    0,    321,  324,  5,
      197,  0,    0,    322,  323,  5,    94,   0,    0,    323,  325,  5,
      69,   0,    0,    324,  322,  1,    0,    0,    0,    324,  325,  1,
      0,    0,    0,    325,  326,  1,    0,    0,    0,    326,  327,  3,
      142,  71,   0,    327,  328,  5,    163,  0,    0,    328,  331,  5,
      32,   0,    0,    329,  330,  5,    94,   0,    0,    330,  332,  5,
      69,   0,    0,    331,  329,  1,    0,    0,    0,    331,  332,  1,
      0,    0,    0,    332,  333,  1,    0,    0,    0,    333,  334,  3,
      154,  77,   0,    334,  335,  5,    205,  0,    0,    335,  336,  3,
      154,  77,   0,    336,  925,  1,    0,    0,    0,    337,  338,  5,
      14,   0,    0,    338,  341,  5,    197,  0,    0,    339,  340,  5,
      94,   0,    0,    340,  342,  5,    69,   0,    0,    341,  339,  1,
      0,    0,    0,    341,  342,  1,    0,    0,    0,    342,  343,  1,
      0,    0,    0,    343,  344,  3,    142,  71,   0,    344,  345,  5,
      59,   0,    0,    345,  348,  5,    32,   0,    0,    346,  347,  5,
      94,   0,    0,    347,  349,  5,    69,   0,    0,    348,  346,  1,
      0,    0,    0,    348,  349,  1,    0,    0,    0,    349,  350,  1,
      0,    0,    0,    350,  351,  3,    142,  71,   0,    351,  925,  1,
      0,    0,    0,    352,  353,  5,    14,   0,    0,    353,  356,  5,
      197,  0,    0,    354,  355,  5,    94,   0,    0,    355,  357,  5,
      69,   0,    0,    356,  354,  1,    0,    0,    0,    356,  357,  1,
      0,    0,    0,    357,  358,  1,    0,    0,    0,    358,  359,  3,
      142,  71,   0,    359,  360,  5,    11,   0,    0,    360,  364,  5,
      32,   0,    0,    361,  362,  5,    94,   0,    0,    362,  363,  5,
      134,  0,    0,    363,  365,  5,    69,   0,    0,    364,  361,  1,
      0,    0,    0,    364,  365,  1,    0,    0,    0,    365,  366,  1,
      0,    0,    0,    366,  367,  3,    14,   7,    0,    367,  925,  1,
      0,    0,    0,    368,  369,  5,    14,   0,    0,    369,  372,  5,
      197,  0,    0,    370,  371,  5,    94,   0,    0,    371,  373,  5,
      69,   0,    0,    372,  370,  1,    0,    0,    0,    372,  373,  1,
      0,    0,    0,    373,  374,  1,    0,    0,    0,    374,  375,  3,
      142,  71,   0,    375,  376,  5,    11,   0,    0,    376,  377,  3,
      158,  79,   0,    377,  925,  1,    0,    0,    0,    378,  379,  5,
      14,   0,    0,    379,  382,  5,    197,  0,    0,    380,  381,  5,
      94,   0,    0,    381,  383,  5,    69,   0,    0,    382,  380,  1,
      0,    0,    0,    382,  383,  1,    0,    0,    0,    383,  384,  1,
      0,    0,    0,    384,  385,  3,    142,  71,   0,    385,  386,  5,
      59,   0,    0,    386,  389,  5,    37,   0,    0,    387,  388,  5,
      94,   0,    0,    388,  390,  5,    69,   0,    0,    389,  387,  1,
      0,    0,    0,    389,  390,  1,    0,    0,    0,    390,  391,  1,
      0,    0,    0,    391,  392,  3,    154,  77,   0,    392,  925,  1,
      0,    0,    0,    393,  394,  5,    14,   0,    0,    394,  397,  5,
      197,  0,    0,    395,  396,  5,    94,   0,    0,    396,  398,  5,
      69,   0,    0,    397,  395,  1,    0,    0,    0,    397,  398,  1,
      0,    0,    0,    398,  399,  1,    0,    0,    0,    399,  400,  3,
      142,  71,   0,    400,  402,  5,    14,   0,    0,    401,  403,  5,
      32,   0,    0,    402,  401,  1,    0,    0,    0,    402,  403,  1,
      0,    0,    0,    403,  404,  1,    0,    0,    0,    404,  405,  3,
      154,  77,   0,    405,  406,  5,    186,  0,    0,    406,  407,  5,
      134,  0,    0,    407,  408,  5,    135,  0,    0,    408,  925,  1,
      0,    0,    0,    409,  410,  5,    14,   0,    0,    410,  413,  5,
      197,  0,    0,    411,  412,  5,    94,   0,    0,    412,  414,  5,
      69,   0,    0,    413,  411,  1,    0,    0,    0,    413,  414,  1,
      0,    0,    0,    414,  415,  1,    0,    0,    0,    415,  416,  3,
      142,  71,   0,    416,  418,  5,    14,   0,    0,    417,  419,  5,
      32,   0,    0,    418,  417,  1,    0,    0,    0,    418,  419,  1,
      0,    0,    0,    419,  420,  1,    0,    0,    0,    420,  421,  3,
      154,  77,   0,    421,  422,  5,    59,   0,    0,    422,  423,  5,
      134,  0,    0,    423,  424,  5,    135,  0,    0,    424,  925,  1,
      0,    0,    0,    425,  426,  5,    14,   0,    0,    426,  429,  5,
      197,  0,    0,    427,  428,  5,    94,   0,    0,    428,  430,  5,
      69,   0,    0,    429,  427,  1,    0,    0,    0,    429,  430,  1,
      0,    0,    0,    430,  431,  1,    0,    0,    0,    431,  432,  3,
      142,  71,   0,    432,  433,  5,    186,  0,    0,    433,  434,  5,
      157,  0,    0,    434,  435,  3,    18,   9,    0,    435,  925,  1,
      0,    0,    0,    436,  437,  5,    15,   0,    0,    437,  440,  3,
      142,  71,   0,    438,  439,  5,    228,  0,    0,    439,  441,  3,
      18,   9,    0,    440,  438,  1,    0,    0,    0,    440,  441,  1,
      0,    0,    0,    441,  925,  1,    0,    0,    0,    442,  443,  5,
      38,   0,    0,    443,  444,  5,    210,  0,    0,    444,  445,  3,
      142,  71,   0,    445,  458,  5,    19,   0,    0,    446,  447,  5,
      2,    0,    0,    447,  452,  3,    22,   11,   0,    448,  449,  5,
      4,    0,    0,    449,  451,  3,    22,   11,   0,    450,  448,  1,
      0,    0,    0,    451,  454,  1,    0,    0,    0,    452,  450,  1,
      0,    0,    0,    452,  453,  1,    0,    0,    0,    453,  455,  1,
      0,    0,    0,    454,  452,  1,    0,    0,    0,    455,  456,  5,
      3,    0,    0,    456,  459,  1,    0,    0,    0,    457,  459,  3,
      114,  57,   0,    458,  446,  1,    0,    0,    0,    458,  457,  1,
      0,    0,    0,    459,  925,  1,    0,    0,    0,    460,  463,  5,
      38,   0,    0,    461,  462,  5,    144,  0,    0,    462,  464,  5,
      165,  0,    0,    463,  461,  1,    0,    0,    0,    463,  464,  1,
      0,    0,    0,    464,  465,  1,    0,    0,    0,    465,  466,  5,
      225,  0,    0,    466,  469,  3,    142,  71,   0,    467,  468,  5,
      182,  0,    0,    468,  470,  7,    1,    0,    0,    469,  467,  1,
      0,    0,    0,    469,  470,  1,    0,    0,    0,    470,  471,  1,
      0,    0,    0,    471,  472,  5,    19,   0,    0,    472,  473,  3,
      8,    4,    0,    473,  925,  1,    0,    0,    0,    474,  475,  5,
      14,   0,    0,    475,  478,  5,    225,  0,    0,    476,  477,  5,
      94,   0,    0,    477,  479,  5,    69,   0,    0,    478,  476,  1,
      0,    0,    0,    478,  479,  1,    0,    0,    0,    479,  480,  1,
      0,    0,    0,    480,  481,  3,    142,  71,   0,    481,  482,  5,
      163,  0,    0,    482,  483,  5,    205,  0,    0,    483,  484,  3,
      142,  71,   0,    484,  925,  1,    0,    0,    0,    485,  486,  5,
      59,   0,    0,    486,  489,  5,    225,  0,    0,    487,  488,  5,
      94,   0,    0,    488,  490,  5,    69,   0,    0,    489,  487,  1,
      0,    0,    0,    489,  490,  1,    0,    0,    0,    490,  491,  1,
      0,    0,    0,    491,  925,  3,    142,  71,   0,    492,  493,  5,
      38,   0,    0,    493,  494,  5,    122,  0,    0,    494,  498,  5,
      225,  0,    0,    495,  496,  5,    94,   0,    0,    496,  497,  5,
      134,  0,    0,    497,  499,  5,    69,   0,    0,    498,  495,  1,
      0,    0,    0,    498,  499,  1,    0,    0,    0,    499,  500,  1,
      0,    0,    0,    500,  503,  3,    142,  71,   0,    501,  502,  5,
      34,   0,    0,    502,  504,  3,    94,   47,   0,    503,  501,  1,
      0,    0,    0,    503,  504,  1,    0,    0,    0,    504,  507,  1,
      0,    0,    0,    505,  506,  5,    228,  0,    0,    506,  508,  3,
      18,   9,    0,    507,  505,  1,    0,    0,    0,    507,  508,  1,
      0,    0,    0,    508,  509,  1,    0,    0,    0,    509,  515,  5,
      19,   0,    0,    510,  516,  3,    8,    4,    0,    511,  512,  5,
      2,    0,    0,    512,  513,  3,    8,    4,    0,    513,  514,  5,
      3,    0,    0,    514,  516,  1,    0,    0,    0,    515,  510,  1,
      0,    0,    0,    515,  511,  1,    0,    0,    0,    516,  925,  1,
      0,    0,    0,    517,  518,  5,    59,   0,    0,    518,  519,  5,
      122,  0,    0,    519,  522,  5,    225,  0,    0,    520,  521,  5,
      94,   0,    0,    521,  523,  5,    69,   0,    0,    522,  520,  1,
      0,    0,    0,    522,  523,  1,    0,    0,    0,    523,  524,  1,
      0,    0,    0,    524,  925,  3,    142,  71,   0,    525,  526,  5,
      161,  0,    0,    526,  527,  5,    122,  0,    0,    527,  528,  5,
      225,  0,    0,    528,  529,  3,    142,  71,   0,    529,  530,  5,
      227,  0,    0,    530,  531,  3,    86,   43,   0,    531,  925,  1,
      0,    0,    0,    532,  535,  5,    38,   0,    0,    533,  534,  5,
      144,  0,    0,    534,  536,  5,    165,  0,    0,    535,  533,  1,
      0,    0,    0,    535,  536,  1,    0,    0,    0,    536,  538,  1,
      0,    0,    0,    537,  539,  5,    200,  0,    0,    538,  537,  1,
      0,    0,    0,    538,  539,  1,    0,    0,    0,    539,  540,  1,
      0,    0,    0,    540,  541,  5,    82,   0,    0,    541,  542,  3,
      142,  71,   0,    542,  551,  5,    2,    0,    0,    543,  548,  3,
      22,   11,   0,    544,  545,  5,    4,    0,    0,    545,  547,  3,
      22,   11,   0,    546,  544,  1,    0,    0,    0,    547,  550,  1,
      0,    0,    0,    548,  546,  1,    0,    0,    0,    548,  549,  1,
      0,    0,    0,    549,  552,  1,    0,    0,    0,    550,  548,  1,
      0,    0,    0,    551,  543,  1,    0,    0,    0,    551,  552,  1,
      0,    0,    0,    552,  553,  1,    0,    0,    0,    553,  554,  5,
      3,    0,    0,    554,  555,  5,    170,  0,    0,    555,  558,  3,
      114,  57,   0,    556,  557,  5,    34,   0,    0,    557,  559,  3,
      94,   47,   0,    558,  556,  1,    0,    0,    0,    558,  559,  1,
      0,    0,    0,    559,  560,  1,    0,    0,    0,    560,  561,  3,
      24,   12,   0,    561,  562,  3,    32,   16,   0,    562,  925,  1,
      0,    0,    0,    563,  564,  5,    14,   0,    0,    564,  565,  5,
      82,   0,    0,    565,  567,  3,    142,  71,   0,    566,  568,  3,
      112,  56,   0,    567,  566,  1,    0,    0,    0,    567,  568,  1,
      0,    0,    0,    568,  569,  1,    0,    0,    0,    569,  570,  3,
      28,   14,   0,    570,  925,  1,    0,    0,    0,    571,  573,  5,
      59,   0,    0,    572,  574,  5,    200,  0,    0,    573,  572,  1,
      0,    0,    0,    573,  574,  1,    0,    0,    0,    574,  575,  1,
      0,    0,    0,    575,  578,  5,    82,   0,    0,    576,  577,  5,
      94,   0,    0,    577,  579,  5,    69,   0,    0,    578,  576,  1,
      0,    0,    0,    578,  579,  1,    0,    0,    0,    579,  580,  1,
      0,    0,    0,    580,  582,  3,    142,  71,   0,    581,  583,  3,
      112,  56,   0,    582,  581,  1,    0,    0,    0,    582,  583,  1,
      0,    0,    0,    583,  925,  1,    0,    0,    0,    584,  585,  5,
      26,   0,    0,    585,  586,  3,    142,  71,   0,    586,  595,  5,
      2,    0,    0,    587,  592,  3,    138,  69,   0,    588,  589,  5,
      4,    0,    0,    589,  591,  3,    138,  69,   0,    590,  588,  1,
      0,    0,    0,    591,  594,  1,    0,    0,    0,    592,  590,  1,
      0,    0,    0,    592,  593,  1,    0,    0,    0,    593,  596,  1,
      0,    0,    0,    594,  592,  1,    0,    0,    0,    595,  587,  1,
      0,    0,    0,    595,  596,  1,    0,    0,    0,    596,  597,  1,
      0,    0,    0,    597,  598,  5,    3,    0,    0,    598,  925,  1,
      0,    0,    0,    599,  600,  5,    38,   0,    0,    600,  601,  5,
      173,  0,    0,    601,  605,  3,    154,  77,   0,    602,  603,  5,
      228,  0,    0,    603,  604,  5,    12,   0,    0,    604,  606,  3,
      148,  74,   0,    605,  602,  1,    0,    0,    0,    605,  606,  1,
      0,    0,    0,    606,  925,  1,    0,    0,    0,    607,  608,  5,
      59,   0,    0,    608,  609,  5,    173,  0,    0,    609,  925,  3,
      154,  77,   0,    610,  611,  5,    84,   0,    0,    611,  612,  3,
      152,  76,   0,    612,  613,  5,    205,  0,    0,    613,  618,  3,
      150,  75,   0,    614,  615,  5,    4,    0,    0,    615,  617,  3,
      150,  75,   0,    616,  614,  1,    0,    0,    0,    617,  620,  1,
      0,    0,    0,    618,  616,  1,    0,    0,    0,    618,  619,  1,
      0,    0,    0,    619,  624,  1,    0,    0,    0,    620,  618,  1,
      0,    0,    0,    621,  622,  5,    228,  0,    0,    622,  623,  5,
      12,   0,    0,    623,  625,  5,    143,  0,    0,    624,  621,  1,
      0,    0,    0,    624,  625,  1,    0,    0,    0,    625,  629,  1,
      0,    0,    0,    626,  627,  5,    85,   0,    0,    627,  628,  5,
      25,   0,    0,    628,  630,  3,    148,  74,   0,    629,  626,  1,
      0,    0,    0,    629,  630,  1,    0,    0,    0,    630,  925,  1,
      0,    0,    0,    631,  635,  5,    171,  0,    0,    632,  633,  5,
      12,   0,    0,    633,  634,  5,    143,  0,    0,    634,  636,  5,
      78,   0,    0,    635,  632,  1,    0,    0,    0,    635,  636,  1,
      0,    0,    0,    636,  637,  1,    0,    0,    0,    637,  638,  3,
      152,  76,   0,    638,  639,  5,    80,   0,    0,    639,  644,  3,
      150,  75,   0,    640,  641,  5,    4,    0,    0,    641,  643,  3,
      150,  75,   0,    642,  640,  1,    0,    0,    0,    643,  646,  1,
      0,    0,    0,    644,  642,  1,    0,    0,    0,    644,  645,  1,
      0,    0,    0,    645,  650,  1,    0,    0,    0,    646,  644,  1,
      0,    0,    0,    647,  648,  5,    85,   0,    0,    648,  649,  5,
      25,   0,    0,    649,  651,  3,    148,  74,   0,    650,  647,  1,
      0,    0,    0,    650,  651,  1,    0,    0,    0,    651,  925,  1,
      0,    0,    0,    652,  653,  5,    186,  0,    0,    653,  657,  5,
      173,  0,    0,    654,  658,  5,    13,   0,    0,    655,  658,  5,
      132,  0,    0,    656,  658,  3,    154,  77,   0,    657,  654,  1,
      0,    0,    0,    657,  655,  1,    0,    0,    0,    657,  656,  1,
      0,    0,    0,    658,  925,  1,    0,    0,    0,    659,  670,  5,
      84,   0,    0,    660,  665,  3,    140,  70,   0,    661,  662,  5,
      4,    0,    0,    662,  664,  3,    140,  70,   0,    663,  661,  1,
      0,    0,    0,    664,  667,  1,    0,    0,    0,    665,  663,  1,
      0,    0,    0,    665,  666,  1,    0,    0,    0,    666,  671,  1,
      0,    0,    0,    667,  665,  1,    0,    0,    0,    668,  669,  5,
      13,   0,    0,    669,  671,  5,    156,  0,    0,    670,  660,  1,
      0,    0,    0,    670,  668,  1,    0,    0,    0,    671,  672,  1,
      0,    0,    0,    672,  674,  5,    140,  0,    0,    673,  675,  5,
      197,  0,    0,    674,  673,  1,    0,    0,    0,    674,  675,  1,
      0,    0,    0,    675,  676,  1,    0,    0,    0,    676,  677,  3,
      142,  71,   0,    677,  678,  5,    205,  0,    0,    678,  682,  3,
      150,  75,   0,    679,  680,  5,    228,  0,    0,    680,  681,  5,
      84,   0,    0,    681,  683,  5,    143,  0,    0,    682,  679,  1,
      0,    0,    0,    682,  683,  1,    0,    0,    0,    683,  925,  1,
      0,    0,    0,    684,  688,  5,    171,  0,    0,    685,  686,  5,
      84,   0,    0,    686,  687,  5,    143,  0,    0,    687,  689,  5,
      78,   0,    0,    688,  685,  1,    0,    0,    0,    688,  689,  1,
      0,    0,    0,    689,  700,  1,    0,    0,    0,    690,  695,  3,
      140,  70,   0,    691,  692,  5,    4,    0,    0,    692,  694,  3,
      140,  70,   0,    693,  691,  1,    0,    0,    0,    694,  697,  1,
      0,    0,    0,    695,  693,  1,    0,    0,    0,    695,  696,  1,
      0,    0,    0,    696,  701,  1,    0,    0,    0,    697,  695,  1,
      0,    0,    0,    698,  699,  5,    13,   0,    0,    699,  701,  5,
      156,  0,    0,    700,  690,  1,    0,    0,    0,    700,  698,  1,
      0,    0,    0,    701,  702,  1,    0,    0,    0,    702,  704,  5,
      140,  0,    0,    703,  705,  5,    197,  0,    0,    704,  703,  1,
      0,    0,    0,    704,  705,  1,    0,    0,    0,    705,  706,  1,
      0,    0,    0,    706,  707,  3,    142,  71,   0,    707,  708,  5,
      80,   0,    0,    708,  709,  3,    150,  75,   0,    709,  925,  1,
      0,    0,    0,    710,  711,  5,    188,  0,    0,    711,  717,  5,
      86,   0,    0,    712,  714,  5,    140,  0,    0,    713,  715,  5,
      197,  0,    0,    714,  713,  1,    0,    0,    0,    714,  715,  1,
      0,    0,    0,    715,  716,  1,    0,    0,    0,    716,  718,  3,
      142,  71,   0,    717,  712,  1,    0,    0,    0,    717,  718,  1,
      0,    0,    0,    718,  925,  1,    0,    0,    0,    719,  721,  5,
      70,   0,    0,    720,  722,  5,    15,   0,    0,    721,  720,  1,
      0,    0,    0,    721,  722,  1,    0,    0,    0,    722,  724,  1,
      0,    0,    0,    723,  725,  5,    223,  0,    0,    724,  723,  1,
      0,    0,    0,    724,  725,  1,    0,    0,    0,    725,  737,  1,
      0,    0,    0,    726,  727,  5,    2,    0,    0,    727,  732,  3,
      132,  66,   0,    728,  729,  5,    4,    0,    0,    729,  731,  3,
      132,  66,   0,    730,  728,  1,    0,    0,    0,    731,  734,  1,
      0,    0,    0,    732,  730,  1,    0,    0,    0,    732,  733,  1,
      0,    0,    0,    733,  735,  1,    0,    0,    0,    734,  732,  1,
      0,    0,    0,    735,  736,  5,    3,    0,    0,    736,  738,  1,
      0,    0,    0,    737,  726,  1,    0,    0,    0,    737,  738,  1,
      0,    0,    0,    738,  739,  1,    0,    0,    0,    739,  925,  3,
      6,    3,    0,    740,  741,  5,    188,  0,    0,    741,  742,  5,
      38,   0,    0,    742,  743,  5,    197,  0,    0,    743,  925,  3,
      142,  71,   0,    744,  745,  5,    188,  0,    0,    745,  746,  5,
      38,   0,    0,    746,  747,  5,    225,  0,    0,    747,  925,  3,
      142,  71,   0,    748,  749,  5,    188,  0,    0,    749,  750,  5,
      38,   0,    0,    750,  751,  5,    122,  0,    0,    751,  752,  5,
      225,  0,    0,    752,  925,  3,    142,  71,   0,    753,  754,  5,
      188,  0,    0,    754,  755,  5,    38,   0,    0,    755,  756,  5,
      82,   0,    0,    756,  758,  3,    142,  71,   0,    757,  759,  3,
      112,  56,   0,    758,  757,  1,    0,    0,    0,    758,  759,  1,
      0,    0,    0,    759,  925,  1,    0,    0,    0,    760,  761,  5,
      188,  0,    0,    761,  764,  5,    198,  0,    0,    762,  763,  7,
      2,    0,    0,    763,  765,  3,    142,  71,   0,    764,  762,  1,
      0,    0,    0,    764,  765,  1,    0,    0,    0,    765,  772,  1,
      0,    0,    0,    766,  767,  5,    116,  0,    0,    767,  770,  3,
      94,   47,   0,    768,  769,  5,    64,   0,    0,    769,  771,  3,
      94,   47,   0,    770,  768,  1,    0,    0,    0,    770,  771,  1,
      0,    0,    0,    771,  773,  1,    0,    0,    0,    772,  766,  1,
      0,    0,    0,    772,  773,  1,    0,    0,    0,    773,  925,  1,
      0,    0,    0,    774,  775,  5,    188,  0,    0,    775,  778,  5,
      180,  0,    0,    776,  777,  7,    2,    0,    0,    777,  779,  3,
      154,  77,   0,    778,  776,  1,    0,    0,    0,    778,  779,  1,
      0,    0,    0,    779,  786,  1,    0,    0,    0,    780,  781,  5,
      116,  0,    0,    781,  784,  3,    94,   47,   0,    782,  783,  5,
      64,   0,    0,    783,  785,  3,    94,   47,   0,    784,  782,  1,
      0,    0,    0,    784,  785,  1,    0,    0,    0,    785,  787,  1,
      0,    0,    0,    786,  780,  1,    0,    0,    0,    786,  787,  1,
      0,    0,    0,    787,  925,  1,    0,    0,    0,    788,  789,  5,
      188,  0,    0,    789,  796,  5,    31,   0,    0,    790,  791,  5,
      116,  0,    0,    791,  794,  3,    94,   47,   0,    792,  793,  5,
      64,   0,    0,    793,  795,  3,    94,   47,   0,    794,  792,  1,
      0,    0,    0,    794,  795,  1,    0,    0,    0,    795,  797,  1,
      0,    0,    0,    796,  790,  1,    0,    0,    0,    796,  797,  1,
      0,    0,    0,    797,  925,  1,    0,    0,    0,    798,  799,  5,
      188,  0,    0,    799,  800,  5,    33,   0,    0,    800,  801,  7,
      2,    0,    0,    801,  925,  3,    142,  71,   0,    802,  803,  5,
      188,  0,    0,    803,  804,  5,    192,  0,    0,    804,  805,  5,
      78,   0,    0,    805,  925,  3,    142,  71,   0,    806,  807,  5,
      188,  0,    0,    807,  808,  5,    192,  0,    0,    808,  809,  5,
      78,   0,    0,    809,  810,  5,    2,    0,    0,    810,  811,  3,
      54,   27,   0,    811,  812,  5,    3,    0,    0,    812,  925,  1,
      0,    0,    0,    813,  815,  5,    188,  0,    0,    814,  816,  5,
      41,   0,    0,    815,  814,  1,    0,    0,    0,    815,  816,  1,
      0,    0,    0,    816,  817,  1,    0,    0,    0,    817,  820,  5,
      174,  0,    0,    818,  819,  7,    2,    0,    0,    819,  821,  3,
      154,  77,   0,    820,  818,  1,    0,    0,    0,    820,  821,  1,
      0,    0,    0,    821,  925,  1,    0,    0,    0,    822,  823,  5,
      188,  0,    0,    823,  824,  5,    173,  0,    0,    824,  827,  5,
      86,   0,    0,    825,  826,  7,    2,    0,    0,    826,  828,  3,
      154,  77,   0,    827,  825,  1,    0,    0,    0,    827,  828,  1,
      0,    0,    0,    828,  925,  1,    0,    0,    0,    829,  830,  5,
      54,   0,    0,    830,  925,  3,    142,  71,   0,    831,  832,  5,
      53,   0,    0,    832,  925,  3,    142,  71,   0,    833,  834,  5,
      188,  0,    0,    834,  841,  5,    83,   0,    0,    835,  836,  5,
      116,  0,    0,    836,  839,  3,    94,   47,   0,    837,  838,  5,
      64,   0,    0,    838,  840,  3,    94,   47,   0,    839,  837,  1,
      0,    0,    0,    839,  840,  1,    0,    0,    0,    840,  842,  1,
      0,    0,    0,    841,  835,  1,    0,    0,    0,    841,  842,  1,
      0,    0,    0,    842,  925,  1,    0,    0,    0,    843,  844,  5,
      188,  0,    0,    844,  851,  5,    185,  0,    0,    845,  846,  5,
      116,  0,    0,    846,  849,  3,    94,   47,   0,    847,  848,  5,
      64,   0,    0,    848,  850,  3,    94,   47,   0,    849,  847,  1,
      0,    0,    0,    849,  850,  1,    0,    0,    0,    850,  852,  1,
      0,    0,    0,    851,  845,  1,    0,    0,    0,    851,  852,  1,
      0,    0,    0,    852,  925,  1,    0,    0,    0,    853,  854,  5,
      186,  0,    0,    854,  855,  5,    185,  0,    0,    855,  856,  3,
      142,  71,   0,    856,  857,  5,    233,  0,    0,    857,  858,  3,
      84,   42,   0,    858,  925,  1,    0,    0,    0,    859,  860,  5,
      166,  0,    0,    860,  861,  5,    185,  0,    0,    861,  925,  3,
      142,  71,   0,    862,  863,  5,    191,  0,    0,    863,  872,  5,
      206,  0,    0,    864,  869,  3,    134,  67,   0,    865,  866,  5,
      4,    0,    0,    866,  868,  3,    134,  67,   0,    867,  865,  1,
      0,    0,    0,    868,  871,  1,    0,    0,    0,    869,  867,  1,
      0,    0,    0,    869,  870,  1,    0,    0,    0,    870,  873,  1,
      0,    0,    0,    871,  869,  1,    0,    0,    0,    872,  864,  1,
      0,    0,    0,    872,  873,  1,    0,    0,    0,    873,  925,  1,
      0,    0,    0,    874,  876,  5,    35,   0,    0,    875,  877,  5,
      229,  0,    0,    876,  875,  1,    0,    0,    0,    876,  877,  1,
      0,    0,    0,    877,  925,  1,    0,    0,    0,    878,  880,  5,
      175,  0,    0,    879,  881,  5,    229,  0,    0,    880,  879,  1,
      0,    0,    0,    880,  881,  1,    0,    0,    0,    881,  925,  1,
      0,    0,    0,    882,  883,  5,    154,  0,    0,    883,  884,  3,
      154,  77,   0,    884,  885,  5,    80,   0,    0,    885,  886,  3,
      6,    3,    0,    886,  925,  1,    0,    0,    0,    887,  888,  5,
      50,   0,    0,    888,  889,  5,    154,  0,    0,    889,  925,  3,
      154,  77,   0,    890,  891,  5,    68,   0,    0,    891,  901,  3,
      154,  77,   0,    892,  893,  5,    220,  0,    0,    893,  898,  3,
      84,   42,   0,    894,  895,  5,    4,    0,    0,    895,  897,  3,
      84,   42,   0,    896,  894,  1,    0,    0,    0,    897,  900,  1,
      0,    0,    0,    898,  896,  1,    0,    0,    0,    898,  899,  1,
      0,    0,    0,    899,  902,  1,    0,    0,    0,    900,  898,  1,
      0,    0,    0,    901,  892,  1,    0,    0,    0,    901,  902,  1,
      0,    0,    0,    902,  925,  1,    0,    0,    0,    903,  904,  5,
      54,   0,    0,    904,  905,  5,    99,   0,    0,    905,  925,  3,
      154,  77,   0,    906,  907,  5,    54,   0,    0,    907,  908,  5,
      148,  0,    0,    908,  925,  3,    154,  77,   0,    909,  910,  5,
      217,  0,    0,    910,  911,  3,    142,  71,   0,    911,  912,  5,
      186,  0,    0,    912,  917,  3,    130,  65,   0,    913,  914,  5,
      4,    0,    0,    914,  916,  3,    130,  65,   0,    915,  913,  1,
      0,    0,    0,    916,  919,  1,    0,    0,    0,    917,  915,  1,
      0,    0,    0,    917,  918,  1,    0,    0,    0,    918,  922,  1,
      0,    0,    0,    919,  917,  1,    0,    0,    0,    920,  921,  5,
      227,  0,    0,    921,  923,  3,    86,   43,   0,    922,  920,  1,
      0,    0,    0,    922,  923,  1,    0,    0,    0,    923,  925,  1,
      0,    0,    0,    924,  187,  1,    0,    0,    0,    924,  188,  1,
      0,    0,    0,    924,  190,  1,    0,    0,    0,    924,  195,  1,
      0,    0,    0,    924,  207,  1,    0,    0,    0,    924,  217,  1,
      0,    0,    0,    924,  224,  1,    0,    0,    0,    924,  258,  1,
      0,    0,    0,    924,  284,  1,    0,    0,    0,    924,  291,  1,
      0,    0,    0,    924,  299,  1,    0,    0,    0,    924,  306,  1,
      0,    0,    0,    924,  309,  1,    0,    0,    0,    924,  320,  1,
      0,    0,    0,    924,  337,  1,    0,    0,    0,    924,  352,  1,
      0,    0,    0,    924,  368,  1,    0,    0,    0,    924,  378,  1,
      0,    0,    0,    924,  393,  1,    0,    0,    0,    924,  409,  1,
      0,    0,    0,    924,  425,  1,    0,    0,    0,    924,  436,  1,
      0,    0,    0,    924,  442,  1,    0,    0,    0,    924,  460,  1,
      0,    0,    0,    924,  474,  1,    0,    0,    0,    924,  485,  1,
      0,    0,    0,    924,  492,  1,    0,    0,    0,    924,  517,  1,
      0,    0,    0,    924,  525,  1,    0,    0,    0,    924,  532,  1,
      0,    0,    0,    924,  563,  1,    0,    0,    0,    924,  571,  1,
      0,    0,    0,    924,  584,  1,    0,    0,    0,    924,  599,  1,
      0,    0,    0,    924,  607,  1,    0,    0,    0,    924,  610,  1,
      0,    0,    0,    924,  631,  1,    0,    0,    0,    924,  652,  1,
      0,    0,    0,    924,  659,  1,    0,    0,    0,    924,  684,  1,
      0,    0,    0,    924,  710,  1,    0,    0,    0,    924,  719,  1,
      0,    0,    0,    924,  740,  1,    0,    0,    0,    924,  744,  1,
      0,    0,    0,    924,  748,  1,    0,    0,    0,    924,  753,  1,
      0,    0,    0,    924,  760,  1,    0,    0,    0,    924,  774,  1,
      0,    0,    0,    924,  788,  1,    0,    0,    0,    924,  798,  1,
      0,    0,    0,    924,  802,  1,    0,    0,    0,    924,  806,  1,
      0,    0,    0,    924,  813,  1,    0,    0,    0,    924,  822,  1,
      0,    0,    0,    924,  829,  1,    0,    0,    0,    924,  831,  1,
      0,    0,    0,    924,  833,  1,    0,    0,    0,    924,  843,  1,
      0,    0,    0,    924,  853,  1,    0,    0,    0,    924,  859,  1,
      0,    0,    0,    924,  862,  1,    0,    0,    0,    924,  874,  1,
      0,    0,    0,    924,  878,  1,    0,    0,    0,    924,  882,  1,
      0,    0,    0,    924,  887,  1,    0,    0,    0,    924,  890,  1,
      0,    0,    0,    924,  903,  1,    0,    0,    0,    924,  906,  1,
      0,    0,    0,    924,  909,  1,    0,    0,    0,    925,  7,    1,
      0,    0,    0,    926,  928,  3,    10,   5,    0,    927,  926,  1,
      0,    0,    0,    927,  928,  1,    0,    0,    0,    928,  929,  1,
      0,    0,    0,    929,  930,  3,    46,   23,   0,    930,  9,    1,
      0,    0,    0,    931,  933,  5,    228,  0,    0,    932,  934,  5,
      160,  0,    0,    933,  932,  1,    0,    0,    0,    933,  934,  1,
      0,    0,    0,    934,  935,  1,    0,    0,    0,    935,  940,  3,
      62,   31,   0,    936,  937,  5,    4,    0,    0,    937,  939,  3,
      62,   31,   0,    938,  936,  1,    0,    0,    0,    939,  942,  1,
      0,    0,    0,    940,  938,  1,    0,    0,    0,    940,  941,  1,
      0,    0,    0,    941,  11,   1,    0,    0,    0,    942,  940,  1,
      0,    0,    0,    943,  947,  3,    158,  79,   0,    944,  947,  3,
      14,   7,    0,    945,  947,  3,    16,   8,    0,    946,  943,  1,
      0,    0,    0,    946,  944,  1,    0,    0,    0,    946,  945,  1,
      0,    0,    0,    947,  13,   1,    0,    0,    0,    948,  949,  3,
      154,  77,   0,    949,  952,  3,    114,  57,   0,    950,  951,  5,
      134,  0,    0,    951,  953,  5,    135,  0,    0,    952,  950,  1,
      0,    0,    0,    952,  953,  1,    0,    0,    0,    953,  956,  1,
      0,    0,    0,    954,  955,  5,    34,   0,    0,    955,  957,  3,
      94,   47,   0,    956,  954,  1,    0,    0,    0,    956,  957,  1,
      0,    0,    0,    957,  960,  1,    0,    0,    0,    958,  959,  5,
      228,  0,    0,    959,  961,  3,    18,   9,    0,    960,  958,  1,
      0,    0,    0,    960,  961,  1,    0,    0,    0,    961,  15,   1,
      0,    0,    0,    962,  963,  5,    116,  0,    0,    963,  966,  3,
      142,  71,   0,    964,  965,  7,    3,    0,    0,    965,  967,  5,
      157,  0,    0,    966,  964,  1,    0,    0,    0,    966,  967,  1,
      0,    0,    0,    967,  17,   1,    0,    0,    0,    968,  969,  5,
      2,    0,    0,    969,  974,  3,    20,   10,   0,    970,  971,  5,
      4,    0,    0,    971,  973,  3,    20,   10,   0,    972,  970,  1,
      0,    0,    0,    973,  976,  1,    0,    0,    0,    974,  972,  1,
      0,    0,    0,    974,  975,  1,    0,    0,    0,    975,  977,  1,
      0,    0,    0,    976,  974,  1,    0,    0,    0,    977,  978,  5,
      3,    0,    0,    978,  19,   1,    0,    0,    0,    979,  980,  3,
      154,  77,   0,    980,  981,  5,    233,  0,    0,    981,  982,  3,
      84,   42,   0,    982,  21,   1,    0,    0,    0,    983,  984,  3,
      154,  77,   0,    984,  985,  3,    114,  57,   0,    985,  23,   1,
      0,    0,    0,    986,  988,  3,    26,   13,   0,    987,  986,  1,
      0,    0,    0,    988,  991,  1,    0,    0,    0,    989,  987,  1,
      0,    0,    0,    989,  990,  1,    0,    0,    0,    990,  25,   1,
      0,    0,    0,    991,  989,  1,    0,    0,    0,    992,  993,  5,
      111,  0,    0,    993,  997,  3,    38,   19,   0,    994,  997,  3,
      40,   20,   0,    995,  997,  3,    42,   21,   0,    996,  992,  1,
      0,    0,    0,    996,  994,  1,    0,    0,    0,    996,  995,  1,
      0,    0,    0,    997,  27,   1,    0,    0,    0,    998,  1000, 3,
      30,   15,   0,    999,  998,  1,    0,    0,    0,    1000, 1003, 1,
      0,    0,    0,    1001, 999,  1,    0,    0,    0,    1001, 1002, 1,
      0,    0,    0,    1002, 29,   1,    0,    0,    0,    1003, 1001, 1,
      0,    0,    0,    1004, 1005, 3,    42,   21,   0,    1005, 31,   1,
      0,    0,    0,    1006, 1009, 3,    34,   17,   0,    1007, 1009, 3,
      36,   18,   0,    1008, 1006, 1,    0,    0,    0,    1008, 1007, 1,
      0,    0,    0,    1009, 33,   1,    0,    0,    0,    1010, 1011, 5,
      169,  0,    0,    1011, 1012, 3,    84,   42,   0,    1012, 35,   1,
      0,    0,    0,    1013, 1016, 5,    72,   0,    0,    1014, 1015, 5,
      125,  0,    0,    1015, 1017, 3,    44,   22,   0,    1016, 1014, 1,
      0,    0,    0,    1016, 1017, 1,    0,    0,    0,    1017, 37,   1,
      0,    0,    0,    1018, 1021, 5,    190,  0,    0,    1019, 1021, 3,
      154,  77,   0,    1020, 1018, 1,    0,    0,    0,    1020, 1019, 1,
      0,    0,    0,    1021, 39,   1,    0,    0,    0,    1022, 1026, 5,
      55,   0,    0,    1023, 1024, 5,    134,  0,    0,    1024, 1026, 5,
      55,   0,    0,    1025, 1022, 1,    0,    0,    0,    1025, 1023, 1,
      0,    0,    0,    1026, 41,   1,    0,    0,    0,    1027, 1028, 5,
      170,  0,    0,    1028, 1029, 5,    135,  0,    0,    1029, 1030, 5,
      140,  0,    0,    1030, 1031, 5,    135,  0,    0,    1031, 1037, 5,
      99,   0,    0,    1032, 1033, 5,    27,   0,    0,    1033, 1034, 5,
      140,  0,    0,    1034, 1035, 5,    135,  0,    0,    1035, 1037, 5,
      99,   0,    0,    1036, 1027, 1,    0,    0,    0,    1036, 1032, 1,
      0,    0,    0,    1037, 43,   1,    0,    0,    0,    1038, 1039, 3,
      154,  77,   0,    1039, 45,   1,    0,    0,    0,    1040, 1051, 3,
      48,   24,   0,    1041, 1042, 5,    145,  0,    0,    1042, 1043, 5,
      25,   0,    0,    1043, 1048, 3,    52,   26,   0,    1044, 1045, 5,
      4,    0,    0,    1045, 1047, 3,    52,   26,   0,    1046, 1044, 1,
      0,    0,    0,    1047, 1050, 1,    0,    0,    0,    1048, 1046, 1,
      0,    0,    0,    1048, 1049, 1,    0,    0,    0,    1049, 1052, 1,
      0,    0,    0,    1050, 1048, 1,    0,    0,    0,    1051, 1041, 1,
      0,    0,    0,    1051, 1052, 1,    0,    0,    0,    1052, 1058, 1,
      0,    0,    0,    1053, 1054, 5,    139,  0,    0,    1054, 1056, 5,
      248,  0,    0,    1055, 1057, 7,    4,    0,    0,    1056, 1055, 1,
      0,    0,    0,    1056, 1057, 1,    0,    0,    0,    1057, 1059, 1,
      0,    0,    0,    1058, 1053, 1,    0,    0,    0,    1058, 1059, 1,
      0,    0,    0,    1059, 1069, 1,    0,    0,    0,    1060, 1061, 5,
      117,  0,    0,    1061, 1068, 7,    5,    0,    0,    1062, 1063, 5,
      74,   0,    0,    1063, 1064, 5,    76,   0,    0,    1064, 1065, 5,
      248,  0,    0,    1065, 1066, 5,    178,  0,    0,    1066, 1068, 5,
      141,  0,    0,    1067, 1060, 1,    0,    0,    0,    1067, 1062, 1,
      0,    0,    0,    1068, 1070, 1,    0,    0,    0,    1069, 1067, 1,
      0,    0,    0,    1069, 1070, 1,    0,    0,    0,    1070, 47,   1,
      0,    0,    0,    1071, 1072, 6,    24,   -1,   0,    1072, 1073, 3,
      50,   25,   0,    1073, 1088, 1,    0,    0,    0,    1074, 1075, 10,
      2,    0,    0,    1075, 1077, 5,    101,  0,    0,    1076, 1078, 3,
      64,   32,   0,    1077, 1076, 1,    0,    0,    0,    1077, 1078, 1,
      0,    0,    0,    1078, 1079, 1,    0,    0,    0,    1079, 1087, 3,
      48,   24,   3,    1080, 1081, 10,   1,    0,    0,    1081, 1083, 7,
      6,    0,    0,    1082, 1084, 3,    64,   32,   0,    1083, 1082, 1,
      0,    0,    0,    1083, 1084, 1,    0,    0,    0,    1084, 1085, 1,
      0,    0,    0,    1085, 1087, 3,    48,   24,   2,    1086, 1074, 1,
      0,    0,    0,    1086, 1080, 1,    0,    0,    0,    1087, 1090, 1,
      0,    0,    0,    1088, 1086, 1,    0,    0,    0,    1088, 1089, 1,
      0,    0,    0,    1089, 49,   1,    0,    0,    0,    1090, 1088, 1,
      0,    0,    0,    1091, 1108, 3,    54,   27,   0,    1092, 1093, 5,
      197,  0,    0,    1093, 1108, 3,    142,  71,   0,    1094, 1095, 5,
      222,  0,    0,    1095, 1100, 3,    84,   42,   0,    1096, 1097, 5,
      4,    0,    0,    1097, 1099, 3,    84,   42,   0,    1098, 1096, 1,
      0,    0,    0,    1099, 1102, 1,    0,    0,    0,    1100, 1098, 1,
      0,    0,    0,    1100, 1101, 1,    0,    0,    0,    1101, 1108, 1,
      0,    0,    0,    1102, 1100, 1,    0,    0,    0,    1103, 1104, 5,
      2,    0,    0,    1104, 1105, 3,    46,   23,   0,    1105, 1106, 5,
      3,    0,    0,    1106, 1108, 1,    0,    0,    0,    1107, 1091, 1,
      0,    0,    0,    1107, 1092, 1,    0,    0,    0,    1107, 1094, 1,
      0,    0,    0,    1107, 1103, 1,    0,    0,    0,    1108, 51,   1,
      0,    0,    0,    1109, 1111, 3,    84,   42,   0,    1110, 1112, 7,
      7,    0,    0,    1111, 1110, 1,    0,    0,    0,    1111, 1112, 1,
      0,    0,    0,    1112, 1115, 1,    0,    0,    0,    1113, 1114, 5,
      137,  0,    0,    1114, 1116, 7,    8,    0,    0,    1115, 1113, 1,
      0,    0,    0,    1115, 1116, 1,    0,    0,    0,    1116, 53,   1,
      0,    0,    0,    1117, 1119, 5,    183,  0,    0,    1118, 1120, 3,
      64,   32,   0,    1119, 1118, 1,    0,    0,    0,    1119, 1120, 1,
      0,    0,    0,    1120, 1121, 1,    0,    0,    0,    1121, 1126, 3,
      66,   33,   0,    1122, 1123, 5,    4,    0,    0,    1123, 1125, 3,
      66,   33,   0,    1124, 1122, 1,    0,    0,    0,    1125, 1128, 1,
      0,    0,    0,    1126, 1124, 1,    0,    0,    0,    1126, 1127, 1,
      0,    0,    0,    1127, 1138, 1,    0,    0,    0,    1128, 1126, 1,
      0,    0,    0,    1129, 1130, 5,    80,   0,    0,    1130, 1135, 3,
      68,   34,   0,    1131, 1132, 5,    4,    0,    0,    1132, 1134, 3,
      68,   34,   0,    1133, 1131, 1,    0,    0,    0,    1134, 1137, 1,
      0,    0,    0,    1135, 1133, 1,    0,    0,    0,    1135, 1136, 1,
      0,    0,    0,    1136, 1139, 1,    0,    0,    0,    1137, 1135, 1,
      0,    0,    0,    1138, 1129, 1,    0,    0,    0,    1138, 1139, 1,
      0,    0,    0,    1139, 1142, 1,    0,    0,    0,    1140, 1141, 5,
      227,  0,    0,    1141, 1143, 3,    86,   43,   0,    1142, 1140, 1,
      0,    0,    0,    1142, 1143, 1,    0,    0,    0,    1143, 1147, 1,
      0,    0,    0,    1144, 1145, 5,    89,   0,    0,    1145, 1146, 5,
      25,   0,    0,    1146, 1148, 3,    56,   28,   0,    1147, 1144, 1,
      0,    0,    0,    1147, 1148, 1,    0,    0,    0,    1148, 1151, 1,
      0,    0,    0,    1149, 1150, 5,    92,   0,    0,    1150, 1152, 3,
      86,   43,   0,    1151, 1149, 1,    0,    0,    0,    1151, 1152, 1,
      0,    0,    0,    1152, 55,   1,    0,    0,    0,    1153, 1155, 3,
      64,   32,   0,    1154, 1153, 1,    0,    0,    0,    1154, 1155, 1,
      0,    0,    0,    1155, 1156, 1,    0,    0,    0,    1156, 1161, 3,
      58,   29,   0,    1157, 1158, 5,    4,    0,    0,    1158, 1160, 3,
      58,   29,   0,    1159, 1157, 1,    0,    0,    0,    1160, 1163, 1,
      0,    0,    0,    1161, 1159, 1,    0,    0,    0,    1161, 1162, 1,
      0,    0,    0,    1162, 57,   1,    0,    0,    0,    1163, 1161, 1,
      0,    0,    0,    1164, 1205, 3,    60,   30,   0,    1165, 1166, 5,
      176,  0,    0,    1166, 1175, 5,    2,    0,    0,    1167, 1172, 3,
      84,   42,   0,    1168, 1169, 5,    4,    0,    0,    1169, 1171, 3,
      84,   42,   0,    1170, 1168, 1,    0,    0,    0,    1171, 1174, 1,
      0,    0,    0,    1172, 1170, 1,    0,    0,    0,    1172, 1173, 1,
      0,    0,    0,    1173, 1176, 1,    0,    0,    0,    1174, 1172, 1,
      0,    0,    0,    1175, 1167, 1,    0,    0,    0,    1175, 1176, 1,
      0,    0,    0,    1176, 1177, 1,    0,    0,    0,    1177, 1205, 5,
      3,    0,    0,    1178, 1179, 5,    40,   0,    0,    1179, 1188, 5,
      2,    0,    0,    1180, 1185, 3,    84,   42,   0,    1181, 1182, 5,
      4,    0,    0,    1182, 1184, 3,    84,   42,   0,    1183, 1181, 1,
      0,    0,    0,    1184, 1187, 1,    0,    0,    0,    1185, 1183, 1,
      0,    0,    0,    1185, 1186, 1,    0,    0,    0,    1186, 1189, 1,
      0,    0,    0,    1187, 1185, 1,    0,    0,    0,    1188, 1180, 1,
      0,    0,    0,    1188, 1189, 1,    0,    0,    0,    1189, 1190, 1,
      0,    0,    0,    1190, 1205, 5,    3,    0,    0,    1191, 1192, 5,
      90,   0,    0,    1192, 1193, 5,    187,  0,    0,    1193, 1194, 5,
      2,    0,    0,    1194, 1199, 3,    60,   30,   0,    1195, 1196, 5,
      4,    0,    0,    1196, 1198, 3,    60,   30,   0,    1197, 1195, 1,
      0,    0,    0,    1198, 1201, 1,    0,    0,    0,    1199, 1197, 1,
      0,    0,    0,    1199, 1200, 1,    0,    0,    0,    1200, 1202, 1,
      0,    0,    0,    1201, 1199, 1,    0,    0,    0,    1202, 1203, 5,
      3,    0,    0,    1203, 1205, 1,    0,    0,    0,    1204, 1164, 1,
      0,    0,    0,    1204, 1165, 1,    0,    0,    0,    1204, 1178, 1,
      0,    0,    0,    1204, 1191, 1,    0,    0,    0,    1205, 59,   1,
      0,    0,    0,    1206, 1215, 5,    2,    0,    0,    1207, 1212, 3,
      84,   42,   0,    1208, 1209, 5,    4,    0,    0,    1209, 1211, 3,
      84,   42,   0,    1210, 1208, 1,    0,    0,    0,    1211, 1214, 1,
      0,    0,    0,    1212, 1210, 1,    0,    0,    0,    1212, 1213, 1,
      0,    0,    0,    1213, 1216, 1,    0,    0,    0,    1214, 1212, 1,
      0,    0,    0,    1215, 1207, 1,    0,    0,    0,    1215, 1216, 1,
      0,    0,    0,    1216, 1217, 1,    0,    0,    0,    1217, 1220, 5,
      3,    0,    0,    1218, 1220, 3,    84,   42,   0,    1219, 1206, 1,
      0,    0,    0,    1219, 1218, 1,    0,    0,    0,    1220, 61,   1,
      0,    0,    0,    1221, 1223, 3,    154,  77,   0,    1222, 1224, 3,
      80,   40,   0,    1223, 1222, 1,    0,    0,    0,    1223, 1224, 1,
      0,    0,    0,    1224, 1225, 1,    0,    0,    0,    1225, 1226, 5,
      19,   0,    0,    1226, 1227, 5,    2,    0,    0,    1227, 1228, 3,
      8,    4,    0,    1228, 1229, 5,    3,    0,    0,    1229, 63,   1,
      0,    0,    0,    1230, 1231, 7,    9,    0,    0,    1231, 65,   1,
      0,    0,    0,    1232, 1237, 3,    84,   42,   0,    1233, 1235, 5,
      19,   0,    0,    1234, 1233, 1,    0,    0,    0,    1234, 1235, 1,
      0,    0,    0,    1235, 1236, 1,    0,    0,    0,    1236, 1238, 3,
      154,  77,   0,    1237, 1234, 1,    0,    0,    0,    1237, 1238, 1,
      0,    0,    0,    1238, 1245, 1,    0,    0,    0,    1239, 1240, 3,
      142,  71,   0,    1240, 1241, 5,    1,    0,    0,    1241, 1242, 5,
      241,  0,    0,    1242, 1245, 1,    0,    0,    0,    1243, 1245, 5,
      241,  0,    0,    1244, 1232, 1,    0,    0,    0,    1244, 1239, 1,
      0,    0,    0,    1244, 1243, 1,    0,    0,    0,    1245, 67,   1,
      0,    0,    0,    1246, 1247, 6,    34,   -1,   0,    1247, 1248, 3,
      74,   37,   0,    1248, 1267, 1,    0,    0,    0,    1249, 1263, 10,
      2,    0,    0,    1250, 1251, 5,    39,   0,    0,    1251, 1252, 5,
      109,  0,    0,    1252, 1264, 3,    74,   37,   0,    1253, 1254, 3,
      70,   35,   0,    1254, 1255, 5,    109,  0,    0,    1255, 1256, 3,
      68,   34,   0,    1256, 1257, 3,    72,   36,   0,    1257, 1264, 1,
      0,    0,    0,    1258, 1259, 5,    126,  0,    0,    1259, 1260, 3,
      70,   35,   0,    1260, 1261, 5,    109,  0,    0,    1261, 1262, 3,
      74,   37,   0,    1262, 1264, 1,    0,    0,    0,    1263, 1250, 1,
      0,    0,    0,    1263, 1253, 1,    0,    0,    0,    1263, 1258, 1,
      0,    0,    0,    1264, 1266, 1,    0,    0,    0,    1265, 1249, 1,
      0,    0,    0,    1266, 1269, 1,    0,    0,    0,    1267, 1265, 1,
      0,    0,    0,    1267, 1268, 1,    0,    0,    0,    1268, 69,   1,
      0,    0,    0,    1269, 1267, 1,    0,    0,    0,    1270, 1272, 5,
      98,   0,    0,    1271, 1270, 1,    0,    0,    0,    1271, 1272, 1,
      0,    0,    0,    1272, 1286, 1,    0,    0,    0,    1273, 1275, 5,
      114,  0,    0,    1274, 1276, 5,    147,  0,    0,    1275, 1274, 1,
      0,    0,    0,    1275, 1276, 1,    0,    0,    0,    1276, 1286, 1,
      0,    0,    0,    1277, 1279, 5,    172,  0,    0,    1278, 1280, 5,
      147,  0,    0,    1279, 1278, 1,    0,    0,    0,    1279, 1280, 1,
      0,    0,    0,    1280, 1286, 1,    0,    0,    0,    1281, 1283, 5,
      81,   0,    0,    1282, 1284, 5,    147,  0,    0,    1283, 1282, 1,
      0,    0,    0,    1283, 1284, 1,    0,    0,    0,    1284, 1286, 1,
      0,    0,    0,    1285, 1271, 1,    0,    0,    0,    1285, 1273, 1,
      0,    0,    0,    1285, 1277, 1,    0,    0,    0,    1285, 1281, 1,
      0,    0,    0,    1286, 71,   1,    0,    0,    0,    1287, 1288, 5,
      140,  0,    0,    1288, 1302, 3,    86,   43,   0,    1289, 1290, 5,
      220,  0,    0,    1290, 1291, 5,    2,    0,    0,    1291, 1296, 3,
      154,  77,   0,    1292, 1293, 5,    4,    0,    0,    1293, 1295, 3,
      154,  77,   0,    1294, 1292, 1,    0,    0,    0,    1295, 1298, 1,
      0,    0,    0,    1296, 1294, 1,    0,    0,    0,    1296, 1297, 1,
      0,    0,    0,    1297, 1299, 1,    0,    0,    0,    1298, 1296, 1,
      0,    0,    0,    1299, 1300, 5,    3,    0,    0,    1300, 1302, 1,
      0,    0,    0,    1301, 1287, 1,    0,    0,    0,    1301, 1289, 1,
      0,    0,    0,    1302, 73,   1,    0,    0,    0,    1303, 1310, 3,
      78,   39,   0,    1304, 1305, 5,    199,  0,    0,    1305, 1306, 3,
      76,   38,   0,    1306, 1307, 5,    2,    0,    0,    1307, 1308, 3,
      84,   42,   0,    1308, 1309, 5,    3,    0,    0,    1309, 1311, 1,
      0,    0,    0,    1310, 1304, 1,    0,    0,    0,    1310, 1311, 1,
      0,    0,    0,    1311, 75,   1,    0,    0,    0,    1312, 1313, 7,
      10,   0,    0,    1313, 77,   1,    0,    0,    0,    1314, 1322, 3,
      82,   41,   0,    1315, 1317, 5,    19,   0,    0,    1316, 1315, 1,
      0,    0,    0,    1316, 1317, 1,    0,    0,    0,    1317, 1318, 1,
      0,    0,    0,    1318, 1320, 3,    154,  77,   0,    1319, 1321, 3,
      80,   40,   0,    1320, 1319, 1,    0,    0,    0,    1320, 1321, 1,
      0,    0,    0,    1321, 1323, 1,    0,    0,    0,    1322, 1316, 1,
      0,    0,    0,    1322, 1323, 1,    0,    0,    0,    1323, 79,   1,
      0,    0,    0,    1324, 1325, 5,    2,    0,    0,    1325, 1330, 3,
      154,  77,   0,    1326, 1327, 5,    4,    0,    0,    1327, 1329, 3,
      154,  77,   0,    1328, 1326, 1,    0,    0,    0,    1329, 1332, 1,
      0,    0,    0,    1330, 1328, 1,    0,    0,    0,    1330, 1331, 1,
      0,    0,    0,    1331, 1333, 1,    0,    0,    0,    1332, 1330, 1,
      0,    0,    0,    1333, 1334, 5,    3,    0,    0,    1334, 81,   1,
      0,    0,    0,    1335, 1337, 3,    142,  71,   0,    1336, 1338, 3,
      144,  72,   0,    1337, 1336, 1,    0,    0,    0,    1337, 1338, 1,
      0,    0,    0,    1338, 1368, 1,    0,    0,    0,    1339, 1340, 5,
      2,    0,    0,    1340, 1341, 3,    8,    4,    0,    1341, 1342, 5,
      3,    0,    0,    1342, 1368, 1,    0,    0,    0,    1343, 1344, 5,
      216,  0,    0,    1344, 1345, 5,    2,    0,    0,    1345, 1350, 3,
      84,   42,   0,    1346, 1347, 5,    4,    0,    0,    1347, 1349, 3,
      84,   42,   0,    1348, 1346, 1,    0,    0,    0,    1349, 1352, 1,
      0,    0,    0,    1350, 1348, 1,    0,    0,    0,    1350, 1351, 1,
      0,    0,    0,    1351, 1353, 1,    0,    0,    0,    1352, 1350, 1,
      0,    0,    0,    1353, 1356, 5,    3,    0,    0,    1354, 1355, 5,
      228,  0,    0,    1355, 1357, 5,    146,  0,    0,    1356, 1354, 1,
      0,    0,    0,    1356, 1357, 1,    0,    0,    0,    1357, 1368, 1,
      0,    0,    0,    1358, 1359, 5,    113,  0,    0,    1359, 1360, 5,
      2,    0,    0,    1360, 1361, 3,    8,    4,    0,    1361, 1362, 5,
      3,    0,    0,    1362, 1368, 1,    0,    0,    0,    1363, 1364, 5,
      2,    0,    0,    1364, 1365, 3,    68,   34,   0,    1365, 1366, 5,
      3,    0,    0,    1366, 1368, 1,    0,    0,    0,    1367, 1335, 1,
      0,    0,    0,    1367, 1339, 1,    0,    0,    0,    1367, 1343, 1,
      0,    0,    0,    1367, 1358, 1,    0,    0,    0,    1367, 1363, 1,
      0,    0,    0,    1368, 83,   1,    0,    0,    0,    1369, 1370, 3,
      86,   43,   0,    1370, 85,   1,    0,    0,    0,    1371, 1372, 6,
      43,   -1,   0,    1372, 1374, 3,    90,   45,   0,    1373, 1375, 3,
      88,   44,   0,    1374, 1373, 1,    0,    0,    0,    1374, 1375, 1,
      0,    0,    0,    1375, 1379, 1,    0,    0,    0,    1376, 1377, 5,
      134,  0,    0,    1377, 1379, 3,    86,   43,   3,    1378, 1371, 1,
      0,    0,    0,    1378, 1376, 1,    0,    0,    0,    1379, 1388, 1,
      0,    0,    0,    1380, 1381, 10,   2,    0,    0,    1381, 1382, 5,
      16,   0,    0,    1382, 1387, 3,    86,   43,   3,    1383, 1384, 10,
      1,    0,    0,    1384, 1385, 5,    144,  0,    0,    1385, 1387, 3,
      86,   43,   2,    1386, 1380, 1,    0,    0,    0,    1386, 1383, 1,
      0,    0,    0,    1387, 1390, 1,    0,    0,    0,    1388, 1386, 1,
      0,    0,    0,    1388, 1389, 1,    0,    0,    0,    1389, 87,   1,
      0,    0,    0,    1390, 1388, 1,    0,    0,    0,    1391, 1392, 3,
      100,  50,   0,    1392, 1393, 3,    90,   45,   0,    1393, 1453, 1,
      0,    0,    0,    1394, 1395, 3,    100,  50,   0,    1395, 1396, 3,
      102,  51,   0,    1396, 1397, 5,    2,    0,    0,    1397, 1398, 3,
      8,    4,    0,    1398, 1399, 5,    3,    0,    0,    1399, 1453, 1,
      0,    0,    0,    1400, 1402, 5,    134,  0,    0,    1401, 1400, 1,
      0,    0,    0,    1401, 1402, 1,    0,    0,    0,    1402, 1403, 1,
      0,    0,    0,    1403, 1404, 5,    24,   0,    0,    1404, 1405, 3,
      90,   45,   0,    1405, 1406, 5,    16,   0,    0,    1406, 1407, 3,
      90,   45,   0,    1407, 1453, 1,    0,    0,    0,    1408, 1410, 5,
      134,  0,    0,    1409, 1408, 1,    0,    0,    0,    1409, 1410, 1,
      0,    0,    0,    1410, 1411, 1,    0,    0,    0,    1411, 1412, 5,
      96,   0,    0,    1412, 1413, 5,    2,    0,    0,    1413, 1418, 3,
      84,   42,   0,    1414, 1415, 5,    4,    0,    0,    1415, 1417, 3,
      84,   42,   0,    1416, 1414, 1,    0,    0,    0,    1417, 1420, 1,
      0,    0,    0,    1418, 1416, 1,    0,    0,    0,    1418, 1419, 1,
      0,    0,    0,    1419, 1421, 1,    0,    0,    0,    1420, 1418, 1,
      0,    0,    0,    1421, 1422, 5,    3,    0,    0,    1422, 1453, 1,
      0,    0,    0,    1423, 1425, 5,    134,  0,    0,    1424, 1423, 1,
      0,    0,    0,    1424, 1425, 1,    0,    0,    0,    1425, 1426, 1,
      0,    0,    0,    1426, 1427, 5,    96,   0,    0,    1427, 1428, 5,
      2,    0,    0,    1428, 1429, 3,    8,    4,    0,    1429, 1430, 5,
      3,    0,    0,    1430, 1453, 1,    0,    0,    0,    1431, 1433, 5,
      134,  0,    0,    1432, 1431, 1,    0,    0,    0,    1432, 1433, 1,
      0,    0,    0,    1433, 1434, 1,    0,    0,    0,    1434, 1435, 5,
      116,  0,    0,    1435, 1438, 3,    90,   45,   0,    1436, 1437, 5,
      64,   0,    0,    1437, 1439, 3,    90,   45,   0,    1438, 1436, 1,
      0,    0,    0,    1438, 1439, 1,    0,    0,    0,    1439, 1453, 1,
      0,    0,    0,    1440, 1442, 5,    106,  0,    0,    1441, 1443, 5,
      134,  0,    0,    1442, 1441, 1,    0,    0,    0,    1442, 1443, 1,
      0,    0,    0,    1443, 1444, 1,    0,    0,    0,    1444, 1453, 5,
      135,  0,    0,    1445, 1447, 5,    106,  0,    0,    1446, 1448, 5,
      134,  0,    0,    1447, 1446, 1,    0,    0,    0,    1447, 1448, 1,
      0,    0,    0,    1448, 1449, 1,    0,    0,    0,    1449, 1450, 5,
      57,   0,    0,    1450, 1451, 5,    80,   0,    0,    1451, 1453, 3,
      90,   45,   0,    1452, 1391, 1,    0,    0,    0,    1452, 1394, 1,
      0,    0,    0,    1452, 1401, 1,    0,    0,    0,    1452, 1409, 1,
      0,    0,    0,    1452, 1424, 1,    0,    0,    0,    1452, 1432, 1,
      0,    0,    0,    1452, 1440, 1,    0,    0,    0,    1452, 1445, 1,
      0,    0,    0,    1453, 89,   1,    0,    0,    0,    1454, 1455, 6,
      45,   -1,   0,    1455, 1459, 3,    92,   46,   0,    1456, 1457, 7,
      11,   0,    0,    1457, 1459, 3,    90,   45,   4,    1458, 1454, 1,
      0,    0,    0,    1458, 1456, 1,    0,    0,    0,    1459, 1474, 1,
      0,    0,    0,    1460, 1461, 10,   3,    0,    0,    1461, 1462, 7,
      12,   0,    0,    1462, 1473, 3,    90,   45,   4,    1463, 1464, 10,
      2,    0,    0,    1464, 1465, 7,    11,   0,    0,    1465, 1473, 3,
      90,   45,   3,    1466, 1467, 10,   1,    0,    0,    1467, 1468, 5,
      244,  0,    0,    1468, 1473, 3,    90,   45,   2,    1469, 1470, 10,
      5,    0,    0,    1470, 1471, 5,    21,   0,    0,    1471, 1473, 3,
      98,   49,   0,    1472, 1460, 1,    0,    0,    0,    1472, 1463, 1,
      0,    0,    0,    1472, 1466, 1,    0,    0,    0,    1472, 1469, 1,
      0,    0,    0,    1473, 1476, 1,    0,    0,    0,    1474, 1472, 1,
      0,    0,    0,    1474, 1475, 1,    0,    0,    0,    1475, 91,   1,
      0,    0,    0,    1476, 1474, 1,    0,    0,    0,    1477, 1478, 6,
      46,   -1,   0,    1478, 1717, 5,    135,  0,    0,    1479, 1717, 3,
      106,  53,   0,    1480, 1481, 3,    114,  57,   0,    1481, 1482, 3,
      94,   47,   0,    1482, 1717, 1,    0,    0,    0,    1483, 1484, 5,
      257,  0,    0,    1484, 1717, 3,    94,   47,   0,    1485, 1717, 3,
      156,  78,   0,    1486, 1717, 3,    104,  52,   0,    1487, 1717, 3,
      94,   47,   0,    1488, 1717, 5,    247,  0,    0,    1489, 1717, 5,
      5,    0,    0,    1490, 1491, 5,    152,  0,    0,    1491, 1492, 5,
      2,    0,    0,    1492, 1493, 3,    90,   45,   0,    1493, 1494, 5,
      96,   0,    0,    1494, 1495, 3,    90,   45,   0,    1495, 1496, 5,
      3,    0,    0,    1496, 1717, 1,    0,    0,    0,    1497, 1498, 5,
      2,    0,    0,    1498, 1501, 3,    84,   42,   0,    1499, 1500, 5,
      4,    0,    0,    1500, 1502, 3,    84,   42,   0,    1501, 1499, 1,
      0,    0,    0,    1502, 1503, 1,    0,    0,    0,    1503, 1501, 1,
      0,    0,    0,    1503, 1504, 1,    0,    0,    0,    1504, 1505, 1,
      0,    0,    0,    1505, 1506, 5,    3,    0,    0,    1506, 1717, 1,
      0,    0,    0,    1507, 1508, 5,    177,  0,    0,    1508, 1509, 5,
      2,    0,    0,    1509, 1514, 3,    84,   42,   0,    1510, 1511, 5,
      4,    0,    0,    1511, 1513, 3,    84,   42,   0,    1512, 1510, 1,
      0,    0,    0,    1513, 1516, 1,    0,    0,    0,    1514, 1512, 1,
      0,    0,    0,    1514, 1515, 1,    0,    0,    0,    1515, 1517, 1,
      0,    0,    0,    1516, 1514, 1,    0,    0,    0,    1517, 1518, 5,
      3,    0,    0,    1518, 1717, 1,    0,    0,    0,    1519, 1520, 3,
      142,  71,   0,    1520, 1521, 5,    2,    0,    0,    1521, 1522, 5,
      241,  0,    0,    1522, 1524, 5,    3,    0,    0,    1523, 1525, 3,
      122,  61,   0,    1524, 1523, 1,    0,    0,    0,    1524, 1525, 1,
      0,    0,    0,    1525, 1527, 1,    0,    0,    0,    1526, 1528, 3,
      124,  62,   0,    1527, 1526, 1,    0,    0,    0,    1527, 1528, 1,
      0,    0,    0,    1528, 1717, 1,    0,    0,    0,    1529, 1530, 3,
      142,  71,   0,    1530, 1542, 5,    2,    0,    0,    1531, 1533, 3,
      64,   32,   0,    1532, 1531, 1,    0,    0,    0,    1532, 1533, 1,
      0,    0,    0,    1533, 1534, 1,    0,    0,    0,    1534, 1539, 3,
      84,   42,   0,    1535, 1536, 5,    4,    0,    0,    1536, 1538, 3,
      84,   42,   0,    1537, 1535, 1,    0,    0,    0,    1538, 1541, 1,
      0,    0,    0,    1539, 1537, 1,    0,    0,    0,    1539, 1540, 1,
      0,    0,    0,    1540, 1543, 1,    0,    0,    0,    1541, 1539, 1,
      0,    0,    0,    1542, 1532, 1,    0,    0,    0,    1542, 1543, 1,
      0,    0,    0,    1543, 1554, 1,    0,    0,    0,    1544, 1545, 5,
      145,  0,    0,    1545, 1546, 5,    25,   0,    0,    1546, 1551, 3,
      52,   26,   0,    1547, 1548, 5,    4,    0,    0,    1548, 1550, 3,
      52,   26,   0,    1549, 1547, 1,    0,    0,    0,    1550, 1553, 1,
      0,    0,    0,    1551, 1549, 1,    0,    0,    0,    1551, 1552, 1,
      0,    0,    0,    1552, 1555, 1,    0,    0,    0,    1553, 1551, 1,
      0,    0,    0,    1554, 1544, 1,    0,    0,    0,    1554, 1555, 1,
      0,    0,    0,    1555, 1556, 1,    0,    0,    0,    1556, 1558, 5,
      3,    0,    0,    1557, 1559, 3,    122,  61,   0,    1558, 1557, 1,
      0,    0,    0,    1558, 1559, 1,    0,    0,    0,    1559, 1564, 1,
      0,    0,    0,    1560, 1562, 3,    96,   48,   0,    1561, 1560, 1,
      0,    0,    0,    1561, 1562, 1,    0,    0,    0,    1562, 1563, 1,
      0,    0,    0,    1563, 1565, 3,    124,  62,   0,    1564, 1561, 1,
      0,    0,    0,    1564, 1565, 1,    0,    0,    0,    1565, 1717, 1,
      0,    0,    0,    1566, 1567, 3,    154,  77,   0,    1567, 1568, 5,
      6,    0,    0,    1568, 1569, 3,    84,   42,   0,    1569, 1717, 1,
      0,    0,    0,    1570, 1579, 5,    2,    0,    0,    1571, 1576, 3,
      154,  77,   0,    1572, 1573, 5,    4,    0,    0,    1573, 1575, 3,
      154,  77,   0,    1574, 1572, 1,    0,    0,    0,    1575, 1578, 1,
      0,    0,    0,    1576, 1574, 1,    0,    0,    0,    1576, 1577, 1,
      0,    0,    0,    1577, 1580, 1,    0,    0,    0,    1578, 1576, 1,
      0,    0,    0,    1579, 1571, 1,    0,    0,    0,    1579, 1580, 1,
      0,    0,    0,    1580, 1581, 1,    0,    0,    0,    1581, 1582, 5,
      3,    0,    0,    1582, 1583, 5,    6,    0,    0,    1583, 1717, 3,
      84,   42,   0,    1584, 1585, 5,    2,    0,    0,    1585, 1586, 3,
      8,    4,    0,    1586, 1587, 5,    3,    0,    0,    1587, 1717, 1,
      0,    0,    0,    1588, 1589, 5,    69,   0,    0,    1589, 1590, 5,
      2,    0,    0,    1590, 1591, 3,    8,    4,    0,    1591, 1592, 5,
      3,    0,    0,    1592, 1717, 1,    0,    0,    0,    1593, 1594, 5,
      29,   0,    0,    1594, 1596, 3,    90,   45,   0,    1595, 1597, 3,
      120,  60,   0,    1596, 1595, 1,    0,    0,    0,    1597, 1598, 1,
      0,    0,    0,    1598, 1596, 1,    0,    0,    0,    1598, 1599, 1,
      0,    0,    0,    1599, 1602, 1,    0,    0,    0,    1600, 1601, 5,
      60,   0,    0,    1601, 1603, 3,    84,   42,   0,    1602, 1600, 1,
      0,    0,    0,    1602, 1603, 1,    0,    0,    0,    1603, 1604, 1,
      0,    0,    0,    1604, 1605, 5,    62,   0,    0,    1605, 1717, 1,
      0,    0,    0,    1606, 1608, 5,    29,   0,    0,    1607, 1609, 3,
      120,  60,   0,    1608, 1607, 1,    0,    0,    0,    1609, 1610, 1,
      0,    0,    0,    1610, 1608, 1,    0,    0,    0,    1610, 1611, 1,
      0,    0,    0,    1611, 1614, 1,    0,    0,    0,    1612, 1613, 5,
      60,   0,    0,    1613, 1615, 3,    84,   42,   0,    1614, 1612, 1,
      0,    0,    0,    1614, 1615, 1,    0,    0,    0,    1615, 1616, 1,
      0,    0,    0,    1616, 1617, 5,    62,   0,    0,    1617, 1717, 1,
      0,    0,    0,    1618, 1619, 5,    30,   0,    0,    1619, 1620, 5,
      2,    0,    0,    1620, 1621, 3,    84,   42,   0,    1621, 1622, 5,
      19,   0,    0,    1622, 1623, 3,    114,  57,   0,    1623, 1624, 5,
      3,    0,    0,    1624, 1717, 1,    0,    0,    0,    1625, 1626, 5,
      209,  0,    0,    1626, 1627, 5,    2,    0,    0,    1627, 1628, 3,
      84,   42,   0,    1628, 1629, 5,    19,   0,    0,    1629, 1630, 3,
      114,  57,   0,    1630, 1631, 5,    3,    0,    0,    1631, 1717, 1,
      0,    0,    0,    1632, 1633, 5,    18,   0,    0,    1633, 1642, 5,
      7,    0,    0,    1634, 1639, 3,    84,   42,   0,    1635, 1636, 5,
      4,    0,    0,    1636, 1638, 3,    84,   42,   0,    1637, 1635, 1,
      0,    0,    0,    1638, 1641, 1,    0,    0,    0,    1639, 1637, 1,
      0,    0,    0,    1639, 1640, 1,    0,    0,    0,    1640, 1643, 1,
      0,    0,    0,    1641, 1639, 1,    0,    0,    0,    1642, 1634, 1,
      0,    0,    0,    1642, 1643, 1,    0,    0,    0,    1643, 1644, 1,
      0,    0,    0,    1644, 1717, 5,    8,    0,    0,    1645, 1717, 3,
      154,  77,   0,    1646, 1717, 5,    42,   0,    0,    1647, 1651, 5,
      44,   0,    0,    1648, 1649, 5,    2,    0,    0,    1649, 1650, 5,
      248,  0,    0,    1650, 1652, 5,    3,    0,    0,    1651, 1648, 1,
      0,    0,    0,    1651, 1652, 1,    0,    0,    0,    1652, 1717, 1,
      0,    0,    0,    1653, 1657, 5,    45,   0,    0,    1654, 1655, 5,
      2,    0,    0,    1655, 1656, 5,    248,  0,    0,    1656, 1658, 5,
      3,    0,    0,    1657, 1654, 1,    0,    0,    0,    1657, 1658, 1,
      0,    0,    0,    1658, 1717, 1,    0,    0,    0,    1659, 1663, 5,
      118,  0,    0,    1660, 1661, 5,    2,    0,    0,    1661, 1662, 5,
      248,  0,    0,    1662, 1664, 5,    3,    0,    0,    1663, 1660, 1,
      0,    0,    0,    1663, 1664, 1,    0,    0,    0,    1664, 1717, 1,
      0,    0,    0,    1665, 1669, 5,    119,  0,    0,    1666, 1667, 5,
      2,    0,    0,    1667, 1668, 5,    248,  0,    0,    1668, 1670, 5,
      3,    0,    0,    1669, 1666, 1,    0,    0,    0,    1669, 1670, 1,
      0,    0,    0,    1670, 1717, 1,    0,    0,    0,    1671, 1717, 5,
      46,   0,    0,    1672, 1673, 5,    193,  0,    0,    1673, 1674, 5,
      2,    0,    0,    1674, 1675, 3,    90,   45,   0,    1675, 1676, 5,
      80,   0,    0,    1676, 1679, 3,    90,   45,   0,    1677, 1678, 5,
      78,   0,    0,    1678, 1680, 3,    90,   45,   0,    1679, 1677, 1,
      0,    0,    0,    1679, 1680, 1,    0,    0,    0,    1680, 1681, 1,
      0,    0,    0,    1681, 1682, 5,    3,    0,    0,    1682, 1717, 1,
      0,    0,    0,    1683, 1684, 5,    133,  0,    0,    1684, 1685, 5,
      2,    0,    0,    1685, 1688, 3,    90,   45,   0,    1686, 1687, 5,
      4,    0,    0,    1687, 1689, 3,    110,  55,   0,    1688, 1686, 1,
      0,    0,    0,    1688, 1689, 1,    0,    0,    0,    1689, 1690, 1,
      0,    0,    0,    1690, 1691, 5,    3,    0,    0,    1691, 1717, 1,
      0,    0,    0,    1692, 1693, 5,    71,   0,    0,    1693, 1694, 5,
      2,    0,    0,    1694, 1695, 3,    154,  77,   0,    1695, 1696, 5,
      80,   0,    0,    1696, 1697, 3,    90,   45,   0,    1697, 1698, 5,
      3,    0,    0,    1698, 1717, 1,    0,    0,    0,    1699, 1700, 5,
      2,    0,    0,    1700, 1701, 3,    84,   42,   0,    1701, 1702, 5,
      3,    0,    0,    1702, 1717, 1,    0,    0,    0,    1703, 1704, 5,
      90,   0,    0,    1704, 1713, 5,    2,    0,    0,    1705, 1710, 3,
      142,  71,   0,    1706, 1707, 5,    4,    0,    0,    1707, 1709, 3,
      142,  71,   0,    1708, 1706, 1,    0,    0,    0,    1709, 1712, 1,
      0,    0,    0,    1710, 1708, 1,    0,    0,    0,    1710, 1711, 1,
      0,    0,    0,    1711, 1714, 1,    0,    0,    0,    1712, 1710, 1,
      0,    0,    0,    1713, 1705, 1,    0,    0,    0,    1713, 1714, 1,
      0,    0,    0,    1714, 1715, 1,    0,    0,    0,    1715, 1717, 5,
      3,    0,    0,    1716, 1477, 1,    0,    0,    0,    1716, 1479, 1,
      0,    0,    0,    1716, 1480, 1,    0,    0,    0,    1716, 1483, 1,
      0,    0,    0,    1716, 1485, 1,    0,    0,    0,    1716, 1486, 1,
      0,    0,    0,    1716, 1487, 1,    0,    0,    0,    1716, 1488, 1,
      0,    0,    0,    1716, 1489, 1,    0,    0,    0,    1716, 1490, 1,
      0,    0,    0,    1716, 1497, 1,    0,    0,    0,    1716, 1507, 1,
      0,    0,    0,    1716, 1519, 1,    0,    0,    0,    1716, 1529, 1,
      0,    0,    0,    1716, 1566, 1,    0,    0,    0,    1716, 1570, 1,
      0,    0,    0,    1716, 1584, 1,    0,    0,    0,    1716, 1588, 1,
      0,    0,    0,    1716, 1593, 1,    0,    0,    0,    1716, 1606, 1,
      0,    0,    0,    1716, 1618, 1,    0,    0,    0,    1716, 1625, 1,
      0,    0,    0,    1716, 1632, 1,    0,    0,    0,    1716, 1645, 1,
      0,    0,    0,    1716, 1646, 1,    0,    0,    0,    1716, 1647, 1,
      0,    0,    0,    1716, 1653, 1,    0,    0,    0,    1716, 1659, 1,
      0,    0,    0,    1716, 1665, 1,    0,    0,    0,    1716, 1671, 1,
      0,    0,    0,    1716, 1672, 1,    0,    0,    0,    1716, 1683, 1,
      0,    0,    0,    1716, 1692, 1,    0,    0,    0,    1716, 1699, 1,
      0,    0,    0,    1716, 1703, 1,    0,    0,    0,    1717, 1728, 1,
      0,    0,    0,    1718, 1719, 10,   14,   0,    0,    1719, 1720, 5,
      7,    0,    0,    1720, 1721, 3,    90,   45,   0,    1721, 1722, 5,
      8,    0,    0,    1722, 1727, 1,    0,    0,    0,    1723, 1724, 10,
      12,   0,    0,    1724, 1725, 5,    1,    0,    0,    1725, 1727, 3,
      154,  77,   0,    1726, 1718, 1,    0,    0,    0,    1726, 1723, 1,
      0,    0,    0,    1727, 1730, 1,    0,    0,    0,    1728, 1726, 1,
      0,    0,    0,    1728, 1729, 1,    0,    0,    0,    1729, 93,   1,
      0,    0,    0,    1730, 1728, 1,    0,    0,    0,    1731, 1738, 5,
      245,  0,    0,    1732, 1735, 5,    246,  0,    0,    1733, 1734, 5,
      211,  0,    0,    1734, 1736, 5,    245,  0,    0,    1735, 1733, 1,
      0,    0,    0,    1735, 1736, 1,    0,    0,    0,    1736, 1738, 1,
      0,    0,    0,    1737, 1731, 1,    0,    0,    0,    1737, 1732, 1,
      0,    0,    0,    1738, 95,   1,    0,    0,    0,    1739, 1740, 5,
      95,   0,    0,    1740, 1744, 5,    137,  0,    0,    1741, 1742, 5,
      167,  0,    0,    1742, 1744, 5,    137,  0,    0,    1743, 1739, 1,
      0,    0,    0,    1743, 1741, 1,    0,    0,    0,    1744, 97,   1,
      0,    0,    0,    1745, 1746, 5,    203,  0,    0,    1746, 1747, 5,
      232,  0,    0,    1747, 1752, 3,    106,  53,   0,    1748, 1749, 5,
      203,  0,    0,    1749, 1750, 5,    232,  0,    0,    1750, 1752, 3,
      94,   47,   0,    1751, 1745, 1,    0,    0,    0,    1751, 1748, 1,
      0,    0,    0,    1752, 99,   1,    0,    0,    0,    1753, 1754, 7,
      13,   0,    0,    1754, 101,  1,    0,    0,    0,    1755, 1756, 7,
      14,   0,    0,    1756, 103,  1,    0,    0,    0,    1757, 1758, 7,
      15,   0,    0,    1758, 105,  1,    0,    0,    0,    1759, 1761, 5,
      102,  0,    0,    1760, 1762, 7,    11,   0,    0,    1761, 1760, 1,
      0,    0,    0,    1761, 1762, 1,    0,    0,    0,    1762, 1763, 1,
      0,    0,    0,    1763, 1764, 3,    94,   47,   0,    1764, 1767, 3,
      108,  54,   0,    1765, 1766, 5,    205,  0,    0,    1766, 1768, 3,
      108,  54,   0,    1767, 1765, 1,    0,    0,    0,    1767, 1768, 1,
      0,    0,    0,    1768, 107,  1,    0,    0,    0,    1769, 1770, 7,
      16,   0,    0,    1770, 109,  1,    0,    0,    0,    1771, 1772, 7,
      17,   0,    0,    1772, 111,  1,    0,    0,    0,    1773, 1782, 5,
      2,    0,    0,    1774, 1779, 3,    114,  57,   0,    1775, 1776, 5,
      4,    0,    0,    1776, 1778, 3,    114,  57,   0,    1777, 1775, 1,
      0,    0,    0,    1778, 1781, 1,    0,    0,    0,    1779, 1777, 1,
      0,    0,    0,    1779, 1780, 1,    0,    0,    0,    1780, 1783, 1,
      0,    0,    0,    1781, 1779, 1,    0,    0,    0,    1782, 1774, 1,
      0,    0,    0,    1782, 1783, 1,    0,    0,    0,    1783, 1784, 1,
      0,    0,    0,    1784, 1785, 5,    3,    0,    0,    1785, 113,  1,
      0,    0,    0,    1786, 1787, 6,    57,   -1,   0,    1787, 1788, 5,
      18,   0,    0,    1788, 1789, 5,    235,  0,    0,    1789, 1790, 3,
      114,  57,   0,    1790, 1791, 5,    237,  0,    0,    1791, 1834, 1,
      0,    0,    0,    1792, 1793, 5,    121,  0,    0,    1793, 1794, 5,
      235,  0,    0,    1794, 1795, 3,    114,  57,   0,    1795, 1796, 5,
      4,    0,    0,    1796, 1797, 3,    114,  57,   0,    1797, 1798, 5,
      237,  0,    0,    1798, 1834, 1,    0,    0,    0,    1799, 1800, 5,
      177,  0,    0,    1800, 1801, 5,    2,    0,    0,    1801, 1802, 3,
      154,  77,   0,    1802, 1809, 3,    114,  57,   0,    1803, 1804, 5,
      4,    0,    0,    1804, 1805, 3,    154,  77,   0,    1805, 1806, 3,
      114,  57,   0,    1806, 1808, 1,    0,    0,    0,    1807, 1803, 1,
      0,    0,    0,    1808, 1811, 1,    0,    0,    0,    1809, 1807, 1,
      0,    0,    0,    1809, 1810, 1,    0,    0,    0,    1810, 1812, 1,
      0,    0,    0,    1811, 1809, 1,    0,    0,    0,    1812, 1813, 5,
      3,    0,    0,    1813, 1834, 1,    0,    0,    0,    1814, 1826, 3,
      118,  59,   0,    1815, 1816, 5,    2,    0,    0,    1816, 1821, 3,
      116,  58,   0,    1817, 1818, 5,    4,    0,    0,    1818, 1820, 3,
      116,  58,   0,    1819, 1817, 1,    0,    0,    0,    1820, 1823, 1,
      0,    0,    0,    1821, 1819, 1,    0,    0,    0,    1821, 1822, 1,
      0,    0,    0,    1822, 1824, 1,    0,    0,    0,    1823, 1821, 1,
      0,    0,    0,    1824, 1825, 5,    3,    0,    0,    1825, 1827, 1,
      0,    0,    0,    1826, 1815, 1,    0,    0,    0,    1826, 1827, 1,
      0,    0,    0,    1827, 1834, 1,    0,    0,    0,    1828, 1829, 5,
      102,  0,    0,    1829, 1830, 3,    108,  54,   0,    1830, 1831, 5,
      205,  0,    0,    1831, 1832, 3,    108,  54,   0,    1832, 1834, 1,
      0,    0,    0,    1833, 1786, 1,    0,    0,    0,    1833, 1792, 1,
      0,    0,    0,    1833, 1799, 1,    0,    0,    0,    1833, 1814, 1,
      0,    0,    0,    1833, 1828, 1,    0,    0,    0,    1834, 1839, 1,
      0,    0,    0,    1835, 1836, 10,   6,    0,    0,    1836, 1838, 5,
      18,   0,    0,    1837, 1835, 1,    0,    0,    0,    1838, 1841, 1,
      0,    0,    0,    1839, 1837, 1,    0,    0,    0,    1839, 1840, 1,
      0,    0,    0,    1840, 115,  1,    0,    0,    0,    1841, 1839, 1,
      0,    0,    0,    1842, 1845, 5,    248,  0,    0,    1843, 1845, 3,
      114,  57,   0,    1844, 1842, 1,    0,    0,    0,    1844, 1843, 1,
      0,    0,    0,    1845, 117,  1,    0,    0,    0,    1846, 1851, 5,
      255,  0,    0,    1847, 1851, 5,    256,  0,    0,    1848, 1851, 5,
      257,  0,    0,    1849, 1851, 3,    142,  71,   0,    1850, 1846, 1,
      0,    0,    0,    1850, 1847, 1,    0,    0,    0,    1850, 1848, 1,
      0,    0,    0,    1850, 1849, 1,    0,    0,    0,    1851, 119,  1,
      0,    0,    0,    1852, 1853, 5,    226,  0,    0,    1853, 1854, 3,
      84,   42,   0,    1854, 1855, 5,    202,  0,    0,    1855, 1856, 3,
      84,   42,   0,    1856, 121,  1,    0,    0,    0,    1857, 1858, 5,
      75,   0,    0,    1858, 1859, 5,    2,    0,    0,    1859, 1860, 5,
      227,  0,    0,    1860, 1861, 3,    86,   43,   0,    1861, 1862, 5,
      3,    0,    0,    1862, 123,  1,    0,    0,    0,    1863, 1864, 5,
      149,  0,    0,    1864, 1875, 5,    2,    0,    0,    1865, 1866, 5,
      150,  0,    0,    1866, 1867, 5,    25,   0,    0,    1867, 1872, 3,
      84,   42,   0,    1868, 1869, 5,    4,    0,    0,    1869, 1871, 3,
      84,   42,   0,    1870, 1868, 1,    0,    0,    0,    1871, 1874, 1,
      0,    0,    0,    1872, 1870, 1,    0,    0,    0,    1872, 1873, 1,
      0,    0,    0,    1873, 1876, 1,    0,    0,    0,    1874, 1872, 1,
      0,    0,    0,    1875, 1865, 1,    0,    0,    0,    1875, 1876, 1,
      0,    0,    0,    1876, 1887, 1,    0,    0,    0,    1877, 1878, 5,
      145,  0,    0,    1878, 1879, 5,    25,   0,    0,    1879, 1884, 3,
      52,   26,   0,    1880, 1881, 5,    4,    0,    0,    1881, 1883, 3,
      52,   26,   0,    1882, 1880, 1,    0,    0,    0,    1883, 1886, 1,
      0,    0,    0,    1884, 1882, 1,    0,    0,    0,    1884, 1885, 1,
      0,    0,    0,    1885, 1888, 1,    0,    0,    0,    1886, 1884, 1,
      0,    0,    0,    1887, 1877, 1,    0,    0,    0,    1887, 1888, 1,
      0,    0,    0,    1888, 1890, 1,    0,    0,    0,    1889, 1891, 3,
      126,  63,   0,    1890, 1889, 1,    0,    0,    0,    1890, 1891, 1,
      0,    0,    0,    1891, 1892, 1,    0,    0,    0,    1892, 1893, 5,
      3,    0,    0,    1893, 125,  1,    0,    0,    0,    1894, 1895, 5,
      158,  0,    0,    1895, 1919, 3,    128,  64,   0,    1896, 1897, 5,
      178,  0,    0,    1897, 1919, 3,    128,  64,   0,    1898, 1899, 5,
      91,   0,    0,    1899, 1919, 3,    128,  64,   0,    1900, 1901, 5,
      158,  0,    0,    1901, 1902, 5,    24,   0,    0,    1902, 1903, 3,
      128,  64,   0,    1903, 1904, 5,    16,   0,    0,    1904, 1905, 3,
      128,  64,   0,    1905, 1919, 1,    0,    0,    0,    1906, 1907, 5,
      178,  0,    0,    1907, 1908, 5,    24,   0,    0,    1908, 1909, 3,
      128,  64,   0,    1909, 1910, 5,    16,   0,    0,    1910, 1911, 3,
      128,  64,   0,    1911, 1919, 1,    0,    0,    0,    1912, 1913, 5,
      91,   0,    0,    1913, 1914, 5,    24,   0,    0,    1914, 1915, 3,
      128,  64,   0,    1915, 1916, 5,    16,   0,    0,    1916, 1917, 3,
      128,  64,   0,    1917, 1919, 1,    0,    0,    0,    1918, 1894, 1,
      0,    0,    0,    1918, 1896, 1,    0,    0,    0,    1918, 1898, 1,
      0,    0,    0,    1918, 1900, 1,    0,    0,    0,    1918, 1906, 1,
      0,    0,    0,    1918, 1912, 1,    0,    0,    0,    1919, 127,  1,
      0,    0,    0,    1920, 1921, 5,    212,  0,    0,    1921, 1930, 5,
      153,  0,    0,    1922, 1923, 5,    212,  0,    0,    1923, 1930, 5,
      77,   0,    0,    1924, 1925, 5,    41,   0,    0,    1925, 1930, 5,
      177,  0,    0,    1926, 1927, 3,    84,   42,   0,    1927, 1928, 7,
      18,   0,    0,    1928, 1930, 1,    0,    0,    0,    1929, 1920, 1,
      0,    0,    0,    1929, 1922, 1,    0,    0,    0,    1929, 1924, 1,
      0,    0,    0,    1929, 1926, 1,    0,    0,    0,    1930, 129,  1,
      0,    0,    0,    1931, 1932, 3,    154,  77,   0,    1932, 1933, 5,
      233,  0,    0,    1933, 1934, 3,    84,   42,   0,    1934, 131,  1,
      0,    0,    0,    1935, 1936, 5,    79,   0,    0,    1936, 1940, 7,
      19,   0,    0,    1937, 1938, 5,    210,  0,    0,    1938, 1940, 7,
      20,   0,    0,    1939, 1935, 1,    0,    0,    0,    1939, 1937, 1,
      0,    0,    0,    1940, 133,  1,    0,    0,    0,    1941, 1942, 5,
      107,  0,    0,    1942, 1943, 5,    115,  0,    0,    1943, 1947, 3,
      136,  68,   0,    1944, 1945, 5,    159,  0,    0,    1945, 1947, 7,
      21,   0,    0,    1946, 1941, 1,    0,    0,    0,    1946, 1944, 1,
      0,    0,    0,    1947, 135,  1,    0,    0,    0,    1948, 1949, 5,
      159,  0,    0,    1949, 1956, 5,    213,  0,    0,    1950, 1951, 5,
      159,  0,    0,    1951, 1956, 5,    36,   0,    0,    1952, 1953, 5,
      164,  0,    0,    1953, 1956, 5,    159,  0,    0,    1954, 1956, 5,
      184,  0,    0,    1955, 1948, 1,    0,    0,    0,    1955, 1950, 1,
      0,    0,    0,    1955, 1952, 1,    0,    0,    0,    1955, 1954, 1,
      0,    0,    0,    1956, 137,  1,    0,    0,    0,    1957, 1963, 3,
      84,   42,   0,    1958, 1959, 3,    154,  77,   0,    1959, 1960, 5,
      9,    0,    0,    1960, 1961, 3,    84,   42,   0,    1961, 1963, 1,
      0,    0,    0,    1962, 1957, 1,    0,    0,    0,    1962, 1958, 1,
      0,    0,    0,    1963, 139,  1,    0,    0,    0,    1964, 1969, 5,
      183,  0,    0,    1965, 1969, 5,    52,   0,    0,    1966, 1969, 5,
      100,  0,    0,    1967, 1969, 3,    154,  77,   0,    1968, 1964, 1,
      0,    0,    0,    1968, 1965, 1,    0,    0,    0,    1968, 1966, 1,
      0,    0,    0,    1968, 1967, 1,    0,    0,    0,    1969, 141,  1,
      0,    0,    0,    1970, 1975, 3,    154,  77,   0,    1971, 1972, 5,
      1,    0,    0,    1972, 1974, 3,    154,  77,   0,    1973, 1971, 1,
      0,    0,    0,    1974, 1977, 1,    0,    0,    0,    1975, 1973, 1,
      0,    0,    0,    1975, 1976, 1,    0,    0,    0,    1976, 143,  1,
      0,    0,    0,    1977, 1975, 1,    0,    0,    0,    1978, 1979, 5,
      78,   0,    0,    1979, 1980, 7,    22,   0,    0,    1980, 1981, 3,
      146,  73,   0,    1981, 1982, 3,    90,   45,   0,    1982, 145,  1,
      0,    0,    0,    1983, 1984, 5,    19,   0,    0,    1984, 1987, 5,
      138,  0,    0,    1985, 1987, 5,    22,   0,    0,    1986, 1983, 1,
      0,    0,    0,    1986, 1985, 1,    0,    0,    0,    1987, 147,  1,
      0,    0,    0,    1988, 1992, 5,    46,   0,    0,    1989, 1992, 5,
      43,   0,    0,    1990, 1992, 3,    150,  75,   0,    1991, 1988, 1,
      0,    0,    0,    1991, 1989, 1,    0,    0,    0,    1991, 1990, 1,
      0,    0,    0,    1992, 149,  1,    0,    0,    0,    1993, 1994, 5,
      219,  0,    0,    1994, 1999, 3,    154,  77,   0,    1995, 1996, 5,
      173,  0,    0,    1996, 1999, 3,    154,  77,   0,    1997, 1999, 3,
      154,  77,   0,    1998, 1993, 1,    0,    0,    0,    1998, 1995, 1,
      0,    0,    0,    1998, 1997, 1,    0,    0,    0,    1999, 151,  1,
      0,    0,    0,    2000, 2005, 3,    154,  77,   0,    2001, 2002, 5,
      4,    0,    0,    2002, 2004, 3,    154,  77,   0,    2003, 2001, 1,
      0,    0,    0,    2004, 2007, 1,    0,    0,    0,    2005, 2003, 1,
      0,    0,    0,    2005, 2006, 1,    0,    0,    0,    2006, 153,  1,
      0,    0,    0,    2007, 2005, 1,    0,    0,    0,    2008, 2014, 5,
      251,  0,    0,    2009, 2014, 5,    253,  0,    0,    2010, 2014, 3,
      176,  88,   0,    2011, 2014, 5,    254,  0,    0,    2012, 2014, 5,
      252,  0,    0,    2013, 2008, 1,    0,    0,    0,    2013, 2009, 1,
      0,    0,    0,    2013, 2010, 1,    0,    0,    0,    2013, 2011, 1,
      0,    0,    0,    2013, 2012, 1,    0,    0,    0,    2014, 155,  1,
      0,    0,    0,    2015, 2019, 5,    249,  0,    0,    2016, 2019, 5,
      250,  0,    0,    2017, 2019, 5,    248,  0,    0,    2018, 2015, 1,
      0,    0,    0,    2018, 2016, 1,    0,    0,    0,    2018, 2017, 1,
      0,    0,    0,    2019, 157,  1,    0,    0,    0,    2020, 2023, 3,
      160,  80,   0,    2021, 2023, 3,    162,  81,   0,    2022, 2020, 1,
      0,    0,    0,    2022, 2021, 1,    0,    0,    0,    2023, 159,  1,
      0,    0,    0,    2024, 2025, 5,    37,   0,    0,    2025, 2026, 3,
      154,  77,   0,    2026, 2027, 3,    162,  81,   0,    2027, 161,  1,
      0,    0,    0,    2028, 2029, 3,    164,  82,   0,    2029, 2031, 3,
      80,   40,   0,    2030, 2032, 3,    166,  83,   0,    2031, 2030, 1,
      0,    0,    0,    2031, 2032, 1,    0,    0,    0,    2032, 163,  1,
      0,    0,    0,    2033, 2037, 5,    215,  0,    0,    2034, 2035, 5,
      155,  0,    0,    2035, 2037, 5,    110,  0,    0,    2036, 2033, 1,
      0,    0,    0,    2036, 2034, 1,    0,    0,    0,    2037, 165,  1,
      0,    0,    0,    2038, 2040, 3,    168,  84,   0,    2039, 2038, 1,
      0,    0,    0,    2040, 2043, 1,    0,    0,    0,    2041, 2039, 1,
      0,    0,    0,    2041, 2042, 1,    0,    0,    0,    2042, 167,  1,
      0,    0,    0,    2043, 2041, 1,    0,    0,    0,    2044, 2048, 3,
      172,  86,   0,    2045, 2048, 3,    170,  85,   0,    2046, 2048, 3,
      174,  87,   0,    2047, 2044, 1,    0,    0,    0,    2047, 2045, 1,
      0,    0,    0,    2047, 2046, 1,    0,    0,    0,    2048, 169,  1,
      0,    0,    0,    2049, 2053, 5,    162,  0,    0,    2050, 2051, 5,
      134,  0,    0,    2051, 2053, 5,    162,  0,    0,    2052, 2049, 1,
      0,    0,    0,    2052, 2050, 1,    0,    0,    0,    2053, 171,  1,
      0,    0,    0,    2054, 2055, 7,    23,   0,    0,    2055, 173,  1,
      0,    0,    0,    2056, 2060, 5,    63,   0,    0,    2057, 2058, 5,
      134,  0,    0,    2058, 2060, 5,    63,   0,    0,    2059, 2056, 1,
      0,    0,    0,    2059, 2057, 1,    0,    0,    0,    2060, 175,  1,
      0,    0,    0,    2061, 2062, 7,    24,   0,    0,    2062, 177,  1,
      0,    0,    0,    262,  200,  205,  211,  215,  229,  233,  237,  241,
      249,  253,  256,  263,  272,  278,  282,  288,  295,  304,  313,  324,
      331,  341,  348,  356,  364,  372,  382,  389,  397,  402,  413,  418,
      429,  440,  452,  458,  463,  469,  478,  489,  498,  503,  507,  515,
      522,  535,  538,  548,  551,  558,  567,  573,  578,  582,  592,  595,
      605,  618,  624,  629,  635,  644,  650,  657,  665,  670,  674,  682,
      688,  695,  700,  704,  714,  717,  721,  724,  732,  737,  758,  764,
      770,  772,  778,  784,  786,  794,  796,  815,  820,  827,  839,  841,
      849,  851,  869,  872,  876,  880,  898,  901,  917,  922,  924,  927,
      933,  940,  946,  952,  956,  960,  966,  974,  989,  996,  1001, 1008,
      1016, 1020, 1025, 1036, 1048, 1051, 1056, 1058, 1067, 1069, 1077, 1083,
      1086, 1088, 1100, 1107, 1111, 1115, 1119, 1126, 1135, 1138, 1142, 1147,
      1151, 1154, 1161, 1172, 1175, 1185, 1188, 1199, 1204, 1212, 1215, 1219,
      1223, 1234, 1237, 1244, 1263, 1267, 1271, 1275, 1279, 1283, 1285, 1296,
      1301, 1310, 1316, 1320, 1322, 1330, 1337, 1350, 1356, 1367, 1374, 1378,
      1386, 1388, 1401, 1409, 1418, 1424, 1432, 1438, 1442, 1447, 1452, 1458,
      1472, 1474, 1503, 1514, 1524, 1527, 1532, 1539, 1542, 1551, 1554, 1558,
      1561, 1564, 1576, 1579, 1598, 1602, 1610, 1614, 1639, 1642, 1651, 1657,
      1663, 1669, 1679, 1688, 1710, 1713, 1716, 1726, 1728, 1735, 1737, 1743,
      1751, 1761, 1767, 1779, 1782, 1809, 1821, 1826, 1833, 1839, 1844, 1850,
      1872, 1875, 1884, 1887, 1890, 1918, 1929, 1939, 1946, 1955, 1962, 1968,
      1975, 1986, 1991, 1998, 2005, 2013, 2018, 2022, 2031, 2036, 2041, 2047,
      2052, 2059};
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
             ((1ULL << _la) & -6508956968051886080) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -1167849768416759981) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -9081967136130105) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & 2161728340487954167) != 0)) {
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
             ((1ULL << _la) & -6508829423092451292) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -1154338969517871109) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -9081967136130049) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -2145727347294473) != 0)) {
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
    setState(989);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == PrestoSqlParser::CALLED

           || _la == PrestoSqlParser::DETERMINISTIC ||
           ((((_la - 111) & ~0x3fULL) == 0) &&
            ((1ULL << (_la - 111)) & 576460752311812097) != 0)) {
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
             ((1ULL << _la) & -6508829423092451292) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -1154338969517871109) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -9081967136130049) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -2145727347294473) != 0)) {
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
             ((1ULL << _la) & -6508829423092451292) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -1154338969517871109) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -9081967136130049) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -2145727347294473) != 0)) {
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
             ((1ULL << _la) & -6508829423092451292) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -1154338969517871109) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -9081967136130049) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -2145727347294473) != 0)) {
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

        setState(1372);
        antlrcpp::downCast<PredicatedContext*>(_localctx)
            ->valueExpressionContext = valueExpression(0);
        setState(1374);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
            _input, 174, _ctx)) {
          case 1: {
            setState(1373);
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
            if (!(((((_la - 241) & ~0x3fULL) == 0) &&
                   ((1ULL << (_la - 241)) & 7) != 0))) {
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
        type(0);
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
             ((1ULL << _la) & -6364714235016595420) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -1154338969517871109) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -9081967136130049) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -2145727347294473) != 0)) {
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
             ((1ULL << _la) & -6508956968051886080) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -1167849768416759981) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -9081967136130105) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & 2161728340487954167) != 0)) {
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
             ((1ULL << _la) & -6508829423092451292) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -1154338969517871109) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -9081967136130049) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & -2145727347294473) != 0)) {
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
             ((1ULL << _la) & -6508956968051886080) != 0) ||
            ((((_la - 66) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 66)) & -1167849768416759981) != 0) ||
            ((((_la - 130) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 130)) & -9081967136130105) != 0) ||
            ((((_la - 194) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 194)) & 2161728340487954167) != 0)) {
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
    if (!(((((_la - 233) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 233)) & 63) != 0))) {
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
          ((((_la - 93) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 93)) & 3221225473) != 0) ||
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
    if (!(((((_la - 127) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 127)) & 15) != 0))) {
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
         ((1ULL << _la) & -6508956968051886080) != 0) ||
        ((((_la - 66) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 66)) & -1167849768416759981) != 0) ||
        ((((_la - 130) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 130)) & -9081967136130105) != 0) ||
        ((((_la - 194) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 194)) & -144114668725739785) != 0)) {
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

std::any PrestoSqlParser::OverContext::accept(tree::ParseTreeVisitor* visitor) {
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
               ((1ULL << (_la - 58)) & 4611826756452614657) != 0) ||
              _la == PrestoSqlParser::OPTIMIZED ||
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
    if (!(((((_la - 195) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 195)) & 536871427) != 0))) {
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
            ((1ULL << _la) & -6845471433603153920) != 0) ||
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

tree::TerminalNode* PrestoSqlParser::NonReservedContext::EXECUTABLE() {
  return getToken(PrestoSqlParser::EXECUTABLE, 0);
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
           ((1ULL << _la) & -6508956968051886080) != 0) ||
          ((((_la - 66) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 66)) & -1167849768416759981) != 0) ||
          ((((_la - 130) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 130)) & -9081967136130105) != 0) ||
          ((((_la - 194) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 194)) & 519350116087) != 0))) {
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

void PrestoSqlParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  prestosqlParserInitialize();
#else
  ::antlr4::internal::call_once(
      prestosqlParserOnceFlag, prestosqlParserInitialize);
#endif
}
