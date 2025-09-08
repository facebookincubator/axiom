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
include_guard(GLOBAL)

set(VELOX_ANTLR4-RUNTIME_VERSION 4.13.2)
set(VELOX_ANTLR4-RUNTIME_BUILD_SHA256_CHECKSUM
    0ed13668906e86dbc0dcddf30fdee68c10203dea4e83852b4edb810821bee3c4)
set(VELOX_ANTLR4-RUNTIME_SOURCE_URL
    "https://www.antlr.org/download/antlr4-cpp-runtime-${VELOX_ANTLR4-RUNTIME_VERSION}-source.zip"
)

velox_resolve_dependency_url(ANTLR4-RUNTIME)

message(STATUS "Building antlr4-runtime from source")

FetchContent_Declare(
  antlr4-runtime
  URL ${VELOX_ANTLR4-RUNTIME_SOURCE_URL}
  URL_HASH ${VELOX_ANTLR4-RUNTIME_BUILD_SHA256_CHECKSUM})

set(ANTLR4_INSTALL
    OFF
    CACHE BOOL "Disable installation for antlr4-runtime")
set(ANTLR_BUILD_CPP_TESTS
    OFF
    CACHE BOOL "Disable tests for antlr4-runtime")

FetchContent_MakeAvailable(antlr4-runtime)
