# Copyright (c) Meta Platforms, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(
  axiom_sql_spark
  SparkFunctions.cpp
  SparkSqlParser.cpp
  grammar/SqlBaseLexer.cpp
  grammar/SqlBaseParser.cpp
  grammar/SqlBaseParserBaseVisitor.cpp
  grammar/SqlBaseParserVisitor.cpp
)

target_link_libraries(
  axiom_sql_spark
  axiom_sql_parsing
  axiom_logical_plan_builder
  velox_type
  velox_type_tz
  velox_exception
  antlr4_shared
  Folly::folly
  fmt::fmt
)

if(AXIOM_BUILD_TESTING)
  add_subdirectory(tests)
endif()
