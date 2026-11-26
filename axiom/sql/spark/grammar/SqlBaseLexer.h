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

// Generated from SqlBaseLexer.g4 by ANTLR 4.13.2

#pragma once

#include "antlr4-runtime.h"

namespace axiom::sql::spark {

class SqlBaseLexer : public antlr4::Lexer {
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

  explicit SqlBaseLexer(antlr4::CharStream* input);

  ~SqlBaseLexer() override;

  /**
   * When true, parser should throw ParseException for unclosed bracketed
   * comment.
   */
  bool has_unclosed_bracketed_comment = false;

  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or
   * letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is
   * followed by digit '3'. For char stream "2.3_", "2.3" is not a valid decimal
   * token, because it is followed by '_'. For char stream "2.3W", "2.3" is not
   * a valid decimal token, because it is followed by 'W'. For char stream
   * "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol
   * '+' which is not a digit or letter or underscore.
   */
  bool isValidDecimal() {
    const size_t nextChar = _input->LA(1);
    return !(
        (nextChar >= 'A' && nextChar <= 'Z') ||
        (nextChar >= '0' && nextChar <= '9') || nextChar == '_');
  }

  /**
   * This method will be called when we see '/*' and try to match it as a
   * bracketed comment. If the next character is '+', it should be parsed as
   * hint later, and we cannot match it as a bracketed comment.
   *
   * Returns true if the next character is '+'.
   */
  bool isHint() {
    return _input->LA(1) == '+';
  }

  /**
   * This method will be called when the character stream ends and try to find
   * out the unclosed bracketed comment. If the method be called, it means the
   * end of the entire character stream match, and we set the flag and fail
   * later.
   */
  void markUnclosedComment() {
    has_unclosed_bracketed_comment = true;
  }

  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  void action(
      antlr4::RuleContext* context,
      size_t ruleIndex,
      size_t actionIndex) override;

  bool sempred(
      antlr4::RuleContext* _localctx,
      size_t ruleIndex,
      size_t predicateIndex) override;

  // By default the static state used to implement the lexer is lazily
  // initialized during the first call to the constructor. You can call this
  // function if you wish to initialize the static state ahead of time.
  static void initialize();

 private:
  // Individual action functions triggered by action() above.
  void BRACKETED_COMMENTAction(
      antlr4::RuleContext* context,
      size_t actionIndex);

  // Individual semantic predicate functions triggered by sempred() above.
  bool EXPONENT_VALUESempred(
      antlr4::RuleContext* _localctx,
      size_t predicateIndex);
  bool DECIMAL_VALUESempred(
      antlr4::RuleContext* _localctx,
      size_t predicateIndex);
  bool FLOAT_LITERALSempred(
      antlr4::RuleContext* _localctx,
      size_t predicateIndex);
  bool DOUBLE_LITERALSempred(
      antlr4::RuleContext* _localctx,
      size_t predicateIndex);
  bool BIGDECIMAL_LITERALSempred(
      antlr4::RuleContext* _localctx,
      size_t predicateIndex);
  bool BRACKETED_COMMENTSempred(
      antlr4::RuleContext* _localctx,
      size_t predicateIndex);
};

} // namespace axiom::sql::spark
